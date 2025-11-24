import os
import asyncio
import aiohttp
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from loguru import logger


class SarvamBatchSTT:
    """Sarvam AI Batch Speech-to-Text with Speaker Diarization."""
    
    def __init__(self, api_key: str, model: str = "saarika:v2.5"):
        self.api_key = api_key
        self.model = model
        self.initiate_url = "https://api.sarvam.ai/speech-to-text/job/v1"
        self.upload_url = "https://api.sarvam.ai/speech-to-text/job/v1/upload-files"
        self.start_url = "https://api.sarvam.ai/speech-to-text/job/v1/{job_id}/start"
        self.status_url = "https://api.sarvam.ai/speech-to-text/job/v1/{job_id}/status"
        self.download_url = "https://api.sarvam.ai/speech-to-text/job/v1/download-files"
        
    async def initiate_job(self, num_speakers: int = 2) -> dict:
        """Step 1: Initiate a batch job."""
        try:
            headers = {
                "api-subscription-key": self.api_key,
                "Content-Type": "application/json"
            }
            
            payload = {
                "job_parameters": {
                    "model": self.model,
                    "with_diarization": True,
                    "num_speakers": num_speakers
                }
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.initiate_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status in [200, 202]:  # 202 = Accepted
                        result = await response.json()
                        job_id = result.get("job_id")
                        logger.info(f"Job initiated successfully. Job ID: {job_id}")
                        return {"success": True, "job_id": job_id}
                    else:
                        error_text = await response.text()
                        logger.error(f"Job initiation failed: {response.status} - {error_text}")
                        return {"success": False, "error": f"API Error {response.status}: {error_text}"}
        except Exception as e:
            logger.error(f"Error initiating job: {e}")
            return {"success": False, "error": str(e)}
    
    async def upload_files(self, job_id: str, audio_files: list) -> dict:
        """Step 2: Upload audio files to the job - get presigned URLs first."""
        try:
            headers = {
                "api-subscription-key": self.api_key,
                "Content-Type": "application/json"
            }
            
            # Prepare file list with names
            file_names = [os.path.basename(f) for f in audio_files]
            
            payload = {
                "job_id": job_id,
                "files": file_names
            }
            
            async with aiohttp.ClientSession() as session:
                # Step 2a: Get presigned URLs
                async with session.post(
                    self.upload_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status in [200, 202]:
                        result = await response.json()

                        
                        # Get upload URLs from response
                        upload_urls = result.get("upload_urls", {})
                        
                        if not upload_urls:
                            logger.info("No upload URLs in response, assuming files registered successfully")
                            return {"success": True, "result": result}
                        
                        # Step 2b: Upload files to presigned URLs
                        for audio_file in audio_files:
                            filename = os.path.basename(audio_file)
                            url_info = upload_urls.get(filename)
                            
                            if not url_info:
                                logger.warning(f"No upload URL for {filename}")
                                continue
                            
                            # Extract the actual URL from the nested structure
                            upload_url = url_info.get("file_url") if isinstance(url_info, dict) else url_info
                            
                            if not upload_url:
                                logger.warning(f"No file_url for {filename}")
                                continue
                            
                            with open(audio_file, "rb") as f:
                                file_data = f.read()
                            
                            async with session.put(
                                upload_url,
                                data=file_data,
                                headers={"x-ms-blob-type": "BlockBlob"},
                                timeout=aiohttp.ClientTimeout(total=300)
                            ) as upload_response:
                                if upload_response.status not in [200, 201, 204]:
                                    logger.error(f"Failed to upload {filename}: {upload_response.status}")
                        

                        return {"success": True, "result": result}
                    else:
                        error_text = await response.text()
                        logger.error(f"File upload failed: {response.status} - {error_text}")
                        return {"success": False, "error": f"API Error {response.status}: {error_text}"}
        except Exception as e:
            logger.error(f"Error uploading files: {e}")
            return {"success": False, "error": str(e)}
    
    async def start_job(self, job_id: str) -> dict:
        """Step 3: Start the batch job processing."""
        try:
            headers = {
                "api-subscription-key": self.api_key
            }
            
            url = self.start_url.format(job_id=job_id)
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status in [200, 202]:
                        result = await response.json()

                        return {"success": True, "result": result}
                    else:
                        error_text = await response.text()
                        logger.error(f"Job start failed: {response.status} - {error_text}")
                        return {"success": False, "error": f"API Error {response.status}: {error_text}"}
        except Exception as e:
            logger.error(f"Error starting job: {e}")
            return {"success": False, "error": str(e)}
    
    async def submit_batch(self, audio_files: list, num_speakers: int = 2) -> dict:
        """Complete batch submission workflow: initiate -> upload -> start."""
        # Step 1: Initiate job
        init_result = await self.initiate_job(num_speakers)
        if not init_result.get("success"):
            return {"success": False, "error": init_result.get("error"), "files": audio_files}
        
        job_id = init_result["job_id"]
        
        # Step 2: Upload files
        upload_result = await self.upload_files(job_id, audio_files)
        if not upload_result.get("success"):
            return {"success": False, "error": upload_result.get("error"), "files": audio_files}
        
        # Step 3: Start job
        start_result = await self.start_job(job_id)
        if not start_result.get("success"):
            return {"success": False, "error": start_result.get("error"), "files": audio_files}
        
        logger.info(f"Batch submitted successfully. Job ID: {job_id}")
        return {"success": True, "job_id": job_id, "files": audio_files}
    
    async def check_status(self, job_id: str) -> dict:
        """Check the status of a batch job."""
        try:
            headers = {
                "api-subscription-key": self.api_key
            }
            
            url = self.status_url.format(job_id=job_id)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status in [200, 202]:
                        result = await response.json()

                        return result
                    else:
                        error_text = await response.text()
                        logger.error(f"Status check failed: {response.status} - {error_text}")
                        return {"status": "error", "error": error_text}
        except Exception as e:
            logger.error(f"Error checking status: {e}")
            return {"status": "error", "error": str(e)}
    
    async def download_results(self, job_id: str, job_details: list) -> dict:
        """Step 5: Download results after job completion."""
        try:
            headers = {
                "api-subscription-key": self.api_key,
                "Content-Type": "application/json"
            }
            
            # Extract output file names from job_details
            output_files = []
            for detail in job_details:
                outputs = detail.get("outputs", [])
                for output in outputs:
                    file_id = output.get("file_id")
                    if file_id:
                        output_files.append(file_id)
            
            payload = {
                "job_id": job_id,
                "files": output_files
            }
            

            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.download_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    if response.status in [200, 202]:
                        result = await response.json()

                        return {"success": True, "results": result}
                    else:
                        error_text = await response.text()
                        logger.error(f"Download failed: {response.status} - {error_text}")
                        return {"success": False, "error": f"API Error {response.status}: {error_text}"}
        except Exception as e:
            logger.error(f"Error downloading results: {e}")
            return {"success": False, "error": str(e)}
    
    async def wait_for_completion(self, job_id: str, poll_interval: int = 10, max_wait: int = 3600) -> dict:
        """Step 4: Poll job status until completion or timeout."""
        elapsed = 0
        
        while elapsed < max_wait:
            status_result = await self.check_status(job_id)
            
            # Try different possible status field names
            status = (
                status_result.get("status") or 
                status_result.get("job_state") or 
                status_result.get("state") or 
                "unknown"
            )
            
            logger.info(f"Job {job_id}: {status}")
            
            # Check for completion (various possible values)
            if status.lower() in ["completed", "complete", "success", "succeeded"]:
                # Download results
                job_details = status_result.get("job_details", [])
                download_result = await self.download_results(job_id, job_details)
                if download_result.get("success"):
                    return {
                        "status": "completed",
                        "results": download_result.get("results", [])
                    }
                else:
                    return {
                        "status": "error",
                        "error": f"Download failed: {download_result.get('error')}"
                    }
            elif status.lower() in ["failed", "error", "failure"]:
                return status_result
            
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        
        logger.error(f"Job {job_id} timed out after {max_wait} seconds")
        return {"status": "timeout", "error": "Job timed out"}
    
    def format_diarized_transcript(self, diarized_data: list) -> str:
        """Format diarized transcript with speaker labels (Customer/Agent)."""
        formatted = []
        
        # Map speaker IDs to labels
        speaker_map = {
            "0": "Customer",
            "1": "Agent",
            "2": "Speaker 3",
            "3": "Speaker 4",
            "4": "Speaker 5",
            "5": "Speaker 6",
            "6": "Speaker 7",
            "7": "Speaker 8"
        }
        
        for entry in diarized_data:
            speaker_id = entry.get("speaker_id", "UNKNOWN")
            speaker_label = speaker_map.get(speaker_id, f"Speaker {speaker_id}")
            transcript = entry.get("transcript", "")
            start_time = entry.get("start_time_seconds", 0)
            
            formatted.append(f"[{start_time:.1f}s] {speaker_label}: {transcript}")
        
        return "\n".join(formatted)


async def process_batch(batch_files: list, batch_num: int, api_key: str, num_speakers: int = 2) -> list:
    """Process a single batch of files."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing Batch {batch_num} ({len(batch_files)} files)")
    logger.info(f"{'='*60}")
    
    stt = SarvamBatchSTT(api_key)
    
    # Submit batch
    submit_result = await stt.submit_batch(batch_files, num_speakers)
    
    if not submit_result.get("success"):
        # Return error results for all files in batch
        return [{
            "File Name": os.path.basename(f),
            "File Path": f,
            "Detected Language": "error",
            "Transcript": "",
            "Diarized Transcript": "",
            "Error": submit_result.get("error", "Submission failed"),
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        } for f in batch_files]
    
    job_id = submit_result["job_id"]
    
    # Wait for completion
    logger.info(f"Waiting for batch {batch_num} to complete...")
    completion_result = await stt.wait_for_completion(job_id)
    
    # Process results
    results = []
    status = completion_result.get("status")
    
    if status == "completed":
        results_data = completion_result.get("results", {})

        
        # Get download URLs for JSON files
        download_urls = results_data.get("download_urls", {})
        job_details = completion_result.get("job_details", [])
        
        # Download and parse each JSON file
        async with aiohttp.ClientSession() as session:
            for idx, audio_file in enumerate(batch_files):
                filename = os.path.basename(audio_file)
                
                # Find the corresponding output file
                json_file_id = f"{idx}.json"
                url_info = download_urls.get(json_file_id, {})
                json_url = url_info.get("file_url") if isinstance(url_info, dict) else url_info
                
                if json_url:
                    try:

                        async with session.get(json_url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                            if response.status == 200:
                                # Read as text first, then parse as JSON (Azure returns octet-stream)
                                text_content = await response.text()
                                transcript_data = json.loads(text_content)
                                

                                
                                # Regular transcript
                                transcript = transcript_data.get("transcript", "") if isinstance(transcript_data, dict) else ""
                                language = transcript_data.get("language_code", "unknown") if isinstance(transcript_data, dict) else "unknown"
                                
                                # Diarized transcript
                                diarized_obj = transcript_data.get("diarized_transcript", {}) if isinstance(transcript_data, dict) else {}
                                diarized_data = diarized_obj.get("entries", []) if isinstance(diarized_obj, dict) else []
                                diarized_text = stt.format_diarized_transcript(diarized_data) if diarized_data else ""
                                
                                results.append({
                                    "File Name": filename,
                                    "File Path": audio_file,
                                    "Detected Language": language,
                                    "Transcript": transcript,
                                    "Diarized Transcript": diarized_text,
                                    "Error": "Success",
                                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                })
                            else:
                                logger.error(f"Failed to download {json_file_id}: {response.status}")
                                results.append({
                                    "File Name": filename,
                                    "File Path": audio_file,
                                    "Detected Language": "error",
                                    "Transcript": "",
                                    "Diarized Transcript": "",
                                    "Error": f"Failed to download result: {response.status}",
                                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                })
                    except Exception as e:
                        logger.error(f"Error downloading result for {filename}: {e}")
                        results.append({
                            "File Name": filename,
                            "File Path": audio_file,
                            "Detected Language": "error",
                            "Transcript": "",
                            "Diarized Transcript": "",
                            "Error": f"Download error: {str(e)}",
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                else:
                    results.append({
                        "File Name": filename,
                        "File Path": audio_file,
                        "Detected Language": "error",
                        "Transcript": "",
                        "Diarized Transcript": "",
                        "Error": "No download URL found",
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
    else:
        error_msg = completion_result.get("error", f"Job {status}")
        results = [{
            "File Name": os.path.basename(f),
            "File Path": f,
            "Detected Language": "error",
            "Transcript": "",
            "Diarized Transcript": "",
            "Error": error_msg,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        } for f in batch_files]
    
    logger.info(f"Batch {batch_num} completed: {len(results)} files processed")
    return results


async def transcribe_folder_batch(
    folder_path: str,
    api_key: str,
    output_excel: str = "transcriptions.xlsx",
    batch_size: int = 50,
    num_speakers: int = 2
):
    """Transcribe all audio files using batch API with concurrent processing."""
    
    # Find all audio files (case-insensitive, no duplicates)
    audio_extensions = ["*.wav", "*.mp3", "*.m4a", "*.flac", "*.ogg"]
    audio_files = set()  # Use set to avoid duplicates
    
    for ext in audio_extensions:
        audio_files.update(Path(folder_path).glob(ext))
        # Also check uppercase
        audio_files.update(Path(folder_path).glob(ext.upper()))
    
    audio_files = sorted([str(f) for f in audio_files])
    
    if not audio_files:
        logger.warning(f"No audio files found in {folder_path}")
        return []
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Found {len(audio_files)} audio files")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Number of speakers: {num_speakers}")
    logger.info(f"{'='*60}\n")
    
    # Split into batches
    batches = [audio_files[i:i + batch_size] for i in range(0, len(audio_files), batch_size)]
    logger.info(f"Split into {len(batches)} batches")
    
    # Process all batches concurrently
    tasks = [
        process_batch(batch, idx + 1, api_key, num_speakers)
        for idx, batch in enumerate(batches)
    ]
    
    logger.info("Submitting all batches concurrently...")
    batch_results = await asyncio.gather(*tasks)
    
    # Flatten results
    all_results = []
    for batch_result in batch_results:
        all_results.extend(batch_result)
    
    # Save to Excel
    if all_results:
        df = pd.DataFrame(all_results)
        df.to_excel(output_excel, index=False, engine='openpyxl')
        logger.info(f"\n✓ All transcriptions saved to {output_excel}")
        
        # Print summary
        success_count = sum(1 for r in all_results if r["Error"] == "Success")
        error_count = len(all_results) - success_count
        
        logger.info(f"\n{'='*60}")
        logger.info(f"SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total files: {len(all_results)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {error_count}")
        logger.info(f"{'='*60}\n")
    
    return all_results


if __name__ == "__main__":
    # Configuration
    API_KEY = "sk_sjr5xw5l_urtKksPsZ9ZBLydh6QdPcUyy"
    INPUT_FOLDER = "audio_input"
    OUTPUT_EXCEL = "transcriptions_batch.xlsx"
    BATCH_SIZE = 20  # Sarvam API limit: max 20 files per job
    NUM_SPEAKERS = 2  # Adjust based on your audio (customer + agent = 2)
    
    # Create folder if it doesn't exist
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    
    # Run batch transcription
    results = asyncio.run(
        transcribe_folder_batch(
            INPUT_FOLDER,
            API_KEY,
            OUTPUT_EXCEL,
            BATCH_SIZE,
            NUM_SPEAKERS
        )
    )
    
    # Print sample results
    if results:
        print("\n" + "="*60)
        print("SAMPLE RESULTS (First 3 files)")
        print("="*60)
        for item in results[:3]:
            print(f"\nFile: {item['File Name']}")
            print(f"Language: {item['Detected Language']}")
            print(f"Status: {item['Error']}")
            if item['Diarized Transcript']:
                print(f"Diarized Transcript:\n{item['Diarized Transcript'][:200]}...")
            print("-"*60)
