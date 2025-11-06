import openai
import os
import requests
from utils.config import osFunc

#Ver como é feito o retrieve na API do facebook
def download_media(media_id, media_type, wpp_token):

    URL_DIALOG_UPLOAD_MEDIA = "https://waba.360dialog.io/v1/media"
    API_DIALOG = wpp_token

    url = URL_DIALOG_UPLOAD_MEDIA + f"/{media_id}"

    header_get_media = {
        'D360-Api-Key': f"{API_DIALOG}",
    }

    try:
        response = requests.get(url, headers=header_get_media)

        if response.status_code == 200:
            media_content = response.content
            with open(f"/tmp/{media_id}.{media_type}", "wb") as media_file:
                media_file.write(media_content)
            return f"/tmp/{media_id}.{media_type}"
        else:
            print(f"ERROR - media.download_media: Falha ao baixar mídia. Response code: {response.text}")
            return False

    except (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects) as e:
        print(f"ERROR - media.download_media: Falha ao baixar mídia {e}")
        return False
    
def transcribe_audio(audio_path, openai_key = openai.api_key): 
    try:
        with open(audio_path, "rb") as audio_file:
            openai.api_key = openai_key
            transcription = openai.Audio.transcribe(
                model="whisper-1", 
                file=audio_file, 
                language="pt",
                response_format="text"
            )

        return transcription
    
    except Exception as e:
        print(f"ERROR - media.download_media: Falha ao transcrever mídia {e}")
        return "Não foi possível transcrever o áudio"