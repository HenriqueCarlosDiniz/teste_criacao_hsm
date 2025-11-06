import openai
import requests
import traceback
import json

def download_media(media_id, media_type, wpp_token):

    URL_DIALOG_UPLOAD_MEDIA = "https://waba-v2.360dialog.io/"
    API_DIALOG = wpp_token

    url = URL_DIALOG_UPLOAD_MEDIA + f"{media_id}"

    header_get_media = {
        'D360-Api-Key': f"{API_DIALOG}",
    }

    try:
        response = requests.get(url, headers=header_get_media)

        if response.status_code == 200:
            media_content = response.content
            new_media_content = media_content.decode('ascii')
            dict_media_content = json.loads(new_media_content)
            url = dict_media_content['url'].replace("'\'","")
            url = url.replace("https://lookaside.fbsbx.com/",URL_DIALOG_UPLOAD_MEDIA)
            try:
                response = requests.get(url, headers=header_get_media)
                actual_media_content = response.content
                with open(f"/tmp/{media_id}.{media_type}", "wb") as media_file:
                    media_file.write(actual_media_content)
                return f"/tmp/{media_id}.{media_type}"
            except (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects) as e:
                print(f"ERROR - media.download_media: mídia expirada {e}")
                return False
        else:
            print(f"ERROR - media.download_media: Falha ao baixar mídia. Response code: {response.text}")
            return False

    except (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects) as e:
        print(f"ERROR - media.download_media: Falha ao baixar mídia {e}")
        return False

def transcribe_audio(audio_path, openai_key = openai.api_key):
    print('audio_path', audio_path)
    print('openai_key', openai_key)

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
        print(traceback.format_exc())
        return "Não foi possível transcrever o áudio"

def main():
    audio = download_media("982804733300590", "ogg", "eeQ5VsySHIWjpOYXJw34eXwMAK")
    transcribe_audio(audio, "")

if __name__=="__main__":
    main()