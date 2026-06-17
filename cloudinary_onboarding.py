#!/usr/bin/env python3
"""Cloudinary onboarding — upload, inspect, and transform a demo image."""
import os

import cloudinary
import cloudinary.api
import cloudinary.uploader
from cloudinary.utils import cloudinary_url

# Configure Cloudinary from environment variables.
# Expected vars are documented in `.env.example`.
cloud_name = os.environ.get("CLOUDINARY_CLOUD_NAME")
api_key = os.environ.get("CLOUDINARY_API_KEY")
api_secret = os.environ.get("CLOUDINARY_API_SECRET")

missing = [k for k, v in [
    ("CLOUDINARY_CLOUD_NAME", cloud_name),
    ("CLOUDINARY_API_KEY", api_key),
    ("CLOUDINARY_API_SECRET", api_secret),
] if not v]
if missing:
    raise RuntimeError(
        "Missing Cloudinary env vars: " + ", ".join(missing) +
        ". Fill them in your .env (or export them) and rerun."
    )

cloudinary.config(
    cloud_name=cloud_name,
    api_key=api_key,
    api_secret=api_secret,
)

# Sample image hosted on Cloudinary's public demo account
DEMO_IMAGE_URL = "https://res.cloudinary.com/demo/image/upload/sample.jpg"

# 1. Upload the image
print("Uploading demo image...")
upload_result = cloudinary.uploader.upload(DEMO_IMAGE_URL)
secure_url = upload_result["secure_url"]
public_id = upload_result["public_id"]
print(f"Secure URL: {secure_url}")
print(f"Public ID:  {public_id}")

# 2. Fetch image metadata
print("\nImage details:")
details = cloudinary.api.resource(public_id)
print(f"  Width:      {details['width']} px")
print(f"  Height:     {details['height']} px")
print(f"  Format:     {details['format']}")
print(f"  Bytes:      {details['bytes']} bytes")

# 3. Build transformed URL
# f_auto — Cloudinary picks the best format for the visitor's browser (e.g. WebP/AVIF)
# q_auto — Cloudinary optimizes quality vs. file size automatically
transformed_url, _ = cloudinary_url(
    public_id,
    fetch_format="auto",
    quality="auto",
    secure=True,
)

print(
    "\nDone! Click link below to see optimized version of the image. "
    "Check the size and the format."
)
print(f"\nTransformed URL:\n{transformed_url}")
