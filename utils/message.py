def calcular_preco_gpt(completion, agent):
    dolar = 5

    tokens_input = completion.usage.prompt_tokens
    tokens_output = completion.usage.completion_tokens

    if agent.model == "gpt-3.5-turbo-1106":
        preco_input = 0.0010
        preco_output = 0.002
    elif agent.model == "gpt-4-1106-preview":
        preco_input = 0.01
        preco_output = 0.03
    else:
        preco_input = 0.03
        preco_output = 0.06

    preco = round((dolar * (tokens_input*preco_input + tokens_output*preco_output)/1000), 5)
    return preco

def is_user_audio(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'voice' in data['messages'][0] and
        'id' in data['messages'][0]['voice']
    )

def is_user_document(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'document' in data['messages'][0] and
        'id' in data['messages'][0]['document']
    )

def is_user_image(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'image' in data['messages'][0] and
        'id' in data['messages'][0]['image']
    )

def is_user_video(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'video' in data['messages'][0] and
        'id' in data['messages'][0]['video']
    )

def is_user_sticker(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'sticker' in data['messages'][0] and
        'id' in data['messages'][0]['sticker']
    )

def is_user_message(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'text' in data['messages'][0] and
        'body' in data['messages'][0]['text']
    )

def is_button_message(data):
    return (
        'messages' in data and
        len(data['messages']) > 0 and
        'button' in data['messages'][0] and
        'text' in data['messages'][0]['button']
    )

def get_user_message(data):
    return data['messages'][0]['text']['body']

def get_button_message(data):
    return data['messages'][0]['button']['text']

def get_audio(data):
    audio_id = data['messages'][0]['voice']['id']
    audio_type = data['messages'][0]['voice']['mime_type']
    return audio_id, audio_type

def get_document(data):
    document_id = data['messages'][0]['document']['id']
    document_type = data['messages'][0]['document']['mime_type']
    document_name = data['messages'][0]['document']['filename']
    return document_id, document_type, document_name

def get_image(data):
    image_id = data['messages'][0]['image']['id']
    image_type = data['messages'][0]['image']['mime_type']
    return image_id, image_type

def get_video(data):
    video_id = data['messages'][0]['video']['id']
    video_type = data['messages'][0]['video']['mime_type']
    return video_id, video_type

def get_sticker(data):
    sticker_id = data['messages'][0]['sticker']['id']
    sticker_type = data['messages'][0]['sticker']['mime_type']
    return sticker_id, sticker_type