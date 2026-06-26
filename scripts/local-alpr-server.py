import dataclasses
import json
import os
import tempfile
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


HOST = os.getenv("LOCAL_ALPR_HOST", "127.0.0.1")
PORT = int(os.getenv("LOCAL_ALPR_PORT", "8080"))
MAX_UPLOAD_BYTES = int(os.getenv("LOCAL_ALPR_MAX_UPLOAD_BYTES", str(8 * 1024 * 1024)))
DETECTOR_MODEL = os.getenv("FAST_ALPR_DETECTOR_MODEL", "yolo-v9-t-384-license-plate-end2end")
OCR_MODEL = os.getenv("FAST_ALPR_OCR_MODEL", "cct-xs-v2-global-model")

ALPR_INSTANCE = None
ALPR_IMPORT_ERROR = None


def get_alpr():
    global ALPR_INSTANCE, ALPR_IMPORT_ERROR
    if ALPR_INSTANCE is not None:
        return ALPR_INSTANCE

    try:
        from fast_alpr import ALPR

        ALPR_INSTANCE = ALPR(detector_model=DETECTOR_MODEL, ocr_model=OCR_MODEL)
        return ALPR_INSTANCE
    except Exception as error:
        ALPR_IMPORT_ERROR = error
        raise


class Handler(BaseHTTPRequestHandler):
    server_version = "AutoInfoLocalALPR/1.0"

    def do_GET(self):
        if self.path.rstrip("/") != "/info":
            self.send_json(404, {"message": "Not found"})
            return

        self.send_json(
            200,
            {
                "version": "local-fast-alpr",
                "detector_model": DETECTOR_MODEL,
                "ocr_model": OCR_MODEL,
                "ready": ALPR_INSTANCE is not None,
                "import_error": str(ALPR_IMPORT_ERROR) if ALPR_IMPORT_ERROR else None,
            },
        )

    def do_POST(self):
        if self.path.rstrip("/") != "/v1/plate-reader":
            self.send_json(404, {"message": "Not found"})
            return

        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length <= 0 or content_length > MAX_UPLOAD_BYTES:
            self.send_json(413, {"message": "Upload is missing or too large"})
            return

        try:
            image = self.read_upload(content_length)
            if not image:
                self.send_json(400, {"message": "Missing upload field"})
                return

            payload = run_prediction(image)
            self.send_json(200, payload)
        except Exception as error:
            self.send_json(500, {"message": str(error)})

    def read_upload(self, content_length):
        body = self.rfile.read(content_length)
        content_type = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            return None

        raw_message = (
            f"Content-Type: {content_type}\r\n"
            "MIME-Version: 1.0\r\n"
            "\r\n"
        ).encode("utf-8") + body
        message = BytesParser(policy=policy.default).parsebytes(raw_message)
        if not message.is_multipart():
            return None

        for part in message.iter_parts():
            disposition = part.get("Content-Disposition", "")
            name = part.get_param("name", header="content-disposition")
            if "form-data" in disposition and name == "upload":
                return part.get_payload(decode=True)

        return None

    def send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        if os.getenv("LOCAL_ALPR_QUIET", "false").lower() == "true":
            return
        super().log_message(fmt, *args)


def run_prediction(image_bytes):
    alpr = get_alpr()
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as handle:
        handle.write(image_bytes)
        temp_path = Path(handle.name)

    try:
        predictions = alpr.predict(str(temp_path))
    finally:
        temp_path.unlink(missing_ok=True)

    return {
        "results": [normalize_prediction(prediction) for prediction in predictions],
        "camera_id": None,
        "filename": "upload.jpg",
    }


def normalize_prediction(prediction):
    data = to_plain(prediction)
    plate = pick_path(data, "plate", "text", "ocr.text", "ocr.plate", "ocr_result.text") or ""
    score = pick_path(data, "score", "confidence", "ocr.confidence", "ocr.score", "ocr_result.confidence")
    region = pick_path(data, "region", "country", "ocr.region", "ocr.country")
    region_score = pick_path(data, "region_confidence", "ocr.region_confidence", "region.score")
    box = normalize_box(pick_path(data, "box", "bbox", "bounding_box", "detection.bounding_box"))

    result = {
        "plate": normalize_plate_text(plate),
        "score": normalize_score(score),
        "candidates": [
            {
                "plate": normalize_plate_text(plate),
                "score": normalize_score(score),
            }
        ],
    }

    if box:
        result["box"] = box

    if region:
        result["region"] = {
            "code": normalize_region_code(region),
            "score": normalize_score(region_score),
        }

    return result


def to_plain(value):
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, dict):
        return {key: to_plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_plain(item) for item in value]
    if hasattr(value, "__dict__"):
        return {key: to_plain(item) for key, item in vars(value).items() if not key.startswith("_")}
    return value


def pick_path(source, *paths):
    for path in paths:
        current = source
        for part in path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                current = None
                break
        if current is not None:
            return current
    return None


def normalize_box(value):
    value = to_plain(value)
    if isinstance(value, (list, tuple)) and len(value) >= 4:
        return {
            "xmin": number_or_none(value[0]),
            "ymin": number_or_none(value[1]),
            "xmax": number_or_none(value[2]),
            "ymax": number_or_none(value[3]),
        }

    if not isinstance(value, dict):
        return None

    xmin = first_number(value, "xmin", "x1", "left", "x")
    ymin = first_number(value, "ymin", "y1", "top", "y")
    xmax = first_number(value, "xmax", "x2", "right")
    ymax = first_number(value, "ymax", "y2", "bottom")
    width = first_number(value, "width", "w")
    height = first_number(value, "height", "h")

    if xmax is None and xmin is not None and width is not None:
        xmax = xmin + width
    if ymax is None and ymin is not None and height is not None:
        ymax = ymin + height

    if None in (xmin, ymin, xmax, ymax):
        return None

    return {
        "xmin": xmin,
        "ymin": ymin,
        "xmax": xmax,
        "ymax": ymax,
    }


def first_number(source, *keys):
    for key in keys:
        if key in source:
            value = number_or_none(source[key])
            if value is not None:
                return value
    return None


def number_or_none(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def normalize_score(value):
    if isinstance(value, (list, tuple)):
        numbers = [number_or_none(item) for item in value]
        numbers = [number for number in numbers if number is not None]
        if not numbers:
            return 0.0
        value = sum(numbers) / len(numbers)

    number = number_or_none(value)
    if number is None:
        return 0.0
    return max(0.0, min(1.0, number / 100.0 if number > 1 else number))


def normalize_region_code(value):
    text = str(value or "").strip().lower()
    if text in {"czech republic", "czechia", "cz"}:
        return "cz"
    return "".join(char for char in text if char.isalnum() or char in {"-", "_"})


def normalize_plate_text(value):
    return "".join(char for char in str(value or "").upper() if char.isalnum())


def main():
    get_alpr()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Local ALPR listening on http://{HOST}:{PORT}/v1/plate-reader/")
    server.serve_forever()


if __name__ == "__main__":
    main()
