from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import asyncio
import httpx
import time
from datetime import datetime
import cv2
import os
import json
from starlette.requests import Request
from log_context import request_id_var, user_id_var
import uuid

from s3_client import ensure_bucket_exists, download_from_s3, upload_json_to_s3, get_output_file_url
from messaging.consumer import AggregatorRelay
from logger import setup_logger


# ---------------- APP ----------------
app = FastAPI(title="Aggregator Service")


from starlette.types import ASGIApp, Receive, Scope, Send

class LogContextMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))
            request_id = headers.get(b"x-request-id", b"").decode("utf-8") if b"x-request-id" in headers else None
            user_id = headers.get(b"x-user-id", b"").decode("utf-8") if b"x-user-id" in headers else None
            
            if not request_id:
                request_id = str(uuid.uuid4())

            request_id_var.set(request_id)
            user_id_var.set(user_id)

            async def send_wrapper(message):
                if message["type"] == "http.response.start":
                    hdrs = list(message.get("headers", []))
                    hdrs.append((b"x-request-id", request_id.encode("utf-8")))
                    message["headers"] = hdrs
                await send(message)

            await self.app(scope, receive, send_wrapper)
            return

        await self.app(scope, receive, send)

app.add_middleware(LogContextMiddleware)


# ---------------- LOGGING ----------------
# Single logger instance for the aggregator service.
# Console → INFO+ (human-readable)   Developer log → /app/logs/aggregator.jsonl (JSON, DEBUG+)
logger = setup_logger("aggregator")


@app.on_event("startup")
def startup_event():
    # --- 5 Logging Levels Demonstrated ---
    logger.debug("Startup: entering startup_event()")                   # DEBUG  – developer detail
    logger.info("Aggregator service starting up...")                    # INFO   – normal milestone
    logger.warning("Connecting to dependencies; may retry if not ready") # WARNING – potential issue
    ensure_bucket_exists()
    try:
        relay = AggregatorRelay()
        relay.start()
        app.state.relay = relay
        logger.info("AggregatorRelay started successfully")             # INFO   – success
    except Exception as e:
        logger.error(f"Failed to start AggregatorRelay: {e}")          # ERROR  – failure
        raise
    logger.critical("Startup complete – service is now accepting requests")  # CRITICAL – key milestone


# ---------------- SERVICE URLS ----------------
FACE_URL      = os.getenv("FACE_SERVICE_URL", "http://face-service:8000/run")
PERSON_ID_URL = os.getenv("PERSON_ID_SERVICE_URL", "http://person-id-service:8000/run")
SCREEN_URL    = os.getenv("SCREEN_SERVICE_URL", "http://screen-service:8000/run")


# ---------------- INPUT MODEL ----------------
class RunRequest(BaseModel):
    s3_key: str
    objects: list[str]
    frame_interval: int = 1  # coarse message every N frames (+ final)
    job_id: str | None = None   # optional; if omitted, generated automatically
    video_id: str | None = None # optional; if omitted, s3_key is used


class RunResponse(BaseModel):
    job_id: str
    message: str
    output_file_url: str


# ---------------- VIDEO METADATA ----------------
def get_video_metadata(local_path: str):
    cap = cv2.VideoCapture(local_path)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    cap.release()
    return width, height, fps


# ---------------- VIDEO DOWNLOAD & METADATA ----------------
def get_metadata_from_s3(s3_key: str, job_id: str):
    local_video_path = f"/tmp/{job_id}.mp4"
    download_from_s3(s3_key, local_video_path)
    width, height, fps = get_video_metadata(local_video_path)
    if os.path.exists(local_video_path):
        os.remove(local_video_path)
    return width, height, fps


# ---------------- SERVICE CALL ----------------
async def call_service(url: str, payload: dict):
    from log_context import request_id_var, user_id_var
    
    headers = {}
    if request_id_var.get():
        headers["X-Request-ID"] = request_id_var.get()
    if user_id_var.get():
        headers["X-User-ID"] = user_id_var.get()
        
    async with httpx.AsyncClient(timeout=None) as client:
        response = await client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()


# ---------------- MERGE LOGIC ----------------
def merge_results(results: dict, s3_key: str, requested_objects: list):

    merged = {
        "video": s3_key,
        "width": None,
        "height": None,
        "fps": None,
        "object_types": [],
        "frames": []
    }

    frames_map = {}
    detected_types = set()

    include_persons  = "persons"  in requested_objects
    include_idcards  = "id_cards" in requested_objects
    include_faces    = "faces"    in requested_objects
    include_screens  = "screens"  in requested_objects

    for service_output in results.values():
        frames = service_output.get("results") or service_output.get("frames")
        if not frames:
            continue

        for frame in frames:
            fid = frame.get("frame", frame.get("frame_id", 0))

            if fid not in frames_map:
                frames_map[fid] = {
                    "frame_id": fid,
                    "persons":  [],
                    "faces":    [],
                    "id_cards": [],
                    "screens":  []
                }

            if include_persons and "persons" in frame and frame["persons"]:
                frames_map[fid]["persons"].extend(frame["persons"])
                detected_types.add("person")

            if include_idcards and "id_cards" in frame and frame["id_cards"]:
                frames_map[fid]["id_cards"].extend(frame["id_cards"])
                detected_types.add("id_card")

            if include_faces and "faces" in frame and frame["faces"]:
                frames_map[fid]["faces"].extend(frame["faces"])
                detected_types.add("face")

            if include_screens and "screens" in frame and frame["screens"]:
                frames_map[fid]["screens"].extend(frame["screens"])
                detected_types.add("screen")

    # Always write all 4 keys in every frame — unrequested ones stay empty arrays
    for fid in sorted(frames_map):
        merged["frames"].append(frames_map[fid])

    merged["object_types"] = list(detected_types)

    return merged


# ---------------- BACKGROUND JOB ----------------
async def process_pipeline(
    job_id: str,
    request: RunRequest,
    video_id: str,
    job_start_timestamp: str,
):
    logger.info(f"Pipeline started | job_id={job_id} | objects={request.objects}")
    logger.debug(f"Fetching video metadata from S3 | s3_key={request.s3_key}")

    width, height, fps = await asyncio.to_thread(get_metadata_from_s3, request.s3_key, job_id)
    logger.debug(f"Video metadata | width={width} height={height} fps={fps}")

    payload = {
        "s3_key": request.s3_key,
        "job_id": job_id,
        "frame_interval": request.frame_interval,
        "video_id": video_id,
        "job_start_timestamp": job_start_timestamp,
    }

    tasks = []

    if "faces" in request.objects:
        tasks.append(call_service(FACE_URL, payload))

    # person-id-service detects both persons and id_cards — call it if either is requested
    if "persons" in request.objects or "id_cards" in request.objects:
        person_payload = {
            **payload,
            "detect_persons": "persons" in request.objects,
            "detect_idcards": "id_cards" in request.objects,
        }
        tasks.append(call_service(PERSON_ID_URL, person_payload))

    if "screens" in request.objects:
        tasks.append(call_service(SCREEN_URL, payload))

    # Wait for services (background only)
    results_list = await asyncio.gather(*tasks)

    results = {}
    idx = 0
    if "faces" in request.objects:
        results["faces"] = results_list[idx]
        idx += 1
    if "persons" in request.objects or "id_cards" in request.objects:
        results["person_id"] = results_list[idx]
        idx += 1
    if "screens" in request.objects:
        results["screens"] = results_list[idx]

    merged = merge_results(results, s3_key=request.s3_key, requested_objects=request.objects)
    merged["width"] = width
    merged["height"] = height
    merged["fps"] = fps

    job_dir = f"/app/jobs/{job_id}"
    os.makedirs(job_dir, exist_ok=True)

    with open(f"{job_dir}/result.json", "w") as f:
        json.dump(merged, f, indent=4)

    upload_json_to_s3(merged, f"{job_id}/result.json")
    logger.info(f"Pipeline complete | job_id={job_id} | result uploaded to S3")


# ---------------- MAIN PIPELINE ----------------
@app.post("/run", response_model=RunResponse, status_code=202)
async def run_pipeline(request: RunRequest, background_tasks: BackgroundTasks) -> RunResponse:

    job_id = request.job_id or f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{int(time.time()*1000)%1000}"
    job_start_timestamp = datetime.utcnow().isoformat()
    video_id = request.video_id or request.s3_key
    job_dir = f"/app/jobs/{job_id}"
    os.makedirs(job_dir, exist_ok=True)

    logger.info(f"Job accepted | job_id={job_id} | video_id={video_id}")

    # Map requested objects to the service names the relay expects
    OBJECT_TO_SERVICE = {"faces": "face", "persons": "person", "id_cards": "person", "screens": "screen"}
    active_services = {OBJECT_TO_SERVICE[o] for o in request.objects if o in OBJECT_TO_SERVICE}
    app.state.relay.register_job(job_id, active_services)

    # Run pipeline and download in background
    background_tasks.add_task(process_pipeline, job_id, request, video_id, job_start_timestamp)

    # Return job id and S3 output file URL
    output_s3_key = f"{job_id}/result.json"
    output_url = get_output_file_url(output_s3_key)

    return RunResponse(
        job_id=job_id,
        message="Job Started",
        output_file_url=output_url,
    )