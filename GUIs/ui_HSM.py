import tkinter as tk
from tkinter import ttk
import json
from tkinter import messagebox
from tkinter import filedialog
import re
import requests
import time
import sys
sys.path.append('../novo_proj')
from utils.send_wpp import enviar_wpp
from GUIs.config import *
from GUIs.connect_db import *
from datetime import datetime
import pytz



class HSM:
    def __init__(self) -> None:
        self.atualizar()

    def atualizar(self):
        self.df = query_to_dataframe("SELECT * FROM HSM")
        self.ID_HSM_list = list(self.df['ID_HSM'].values)
        self.ID_HSM_ativos_list = list(self.df[self.df['ATIVO'] != 0]['ID_HSM'].values)

class Flow:
    def __init__(self) -> None:
        self.atualizar()

    def atualizar(self):
        self.df = query_to_dataframe("SELECT * FROM WHATSAPP_FLOWS")
        self.ID_FLOW_list = list(self.df['NOME_FLOW'].values)
        self.ID_FLOW_ativos_list = list(self.df[self.df['ATIVO'] != 0]['NOME_FLOW'].values)

def search(event, lst, combobox): 
    value = event.widget.get()

    if value == '':
        combobox['values'] = lst
    else:
        data = []

        for item in lst:
            if value.lower() in item.lower():
                data.append(item)
        
        combobox['values'] = data

def atualizar(combo, HSM_class):
    HSM_class.atualizar()
    combo['values'] = HSM_class.ID_HSM_list
 
def open_file_dialog(URL_video): 
    video_dir = filedialog.askopenfilename()
    URL_video.delete("1.0", tk.END)
    URL_video.insert("1.0", video_dir)
      
def enviar_HSM(selected_options, df, telefone):

    URL_DIALOG_MESSAGE = osFunc('URL_DIALOG_MESSAGE')
    print(URL_DIALOG_MESSAGE)

    HEADERS = {
        'D360-Api-Key': osFunc('API_DIALOG'), #os.environ['API_DIALOG'], #os.environ['API_DIALOG'],
        'Content-Type': "application/json",
    }
    print(HEADERS)
    

    HSM = selected_options.get()
    payload = df[df['ID_HSM'] == HSM].iloc[0]['MESSAGE_PAYLOAD']

    telefone = telefone.get()

    enviar_wpp(telefone=telefone,
               msg = HSM,
               header=HEADERS
               )

    print(payload)

    
    
    payload = payload.replace('{telefone}', str(telefone)) \
        .replace('{cliente}', str("\""+"cliente"+"\"")) \
        .replace('{amigo}', str("\""+'amigo'+"\"")) \
        .replace('{data_agendamento}', str("\""+'data_agendamento'+"\"")) \
        .replace('{data_agendamento2}', str("\""+'data_agendamento2'+"\"")) \
        .replace('{horario_agendamento}', str("\""+'horario_agendamento'+"\"")) \
        .replace('{horario_agendamento2}', str("\""+'horario_agendamento2'+"\"")) \
        .replace('{unidade_agendamento}', str("\""+'unidade_agendamento'+"\"")) \
        .replace('{endereco_agendamento}', str("\""+'endereco_agendamento'+"\"")) \
        .replace('{string_desconto}', str("\""+'string_desconto'+"\"")) \
        .replace('{dor_principal}', str("\""+'dor_principal'+"\"")) \
        .replace('{codigo_rastreio}', str("\""+'link_rastreio'+"\"")) \
        .replace('{nome_especialista}', str("\""+'nome_especialista'+"\"")) \
        .replace('{endereco_agendamento2}', str("\""+'endereco_agendamento2'+"\"")) \
        .replace('{unidade_nome}', str("\""+'unidade_nome'+"\"")) \
        .replace('{date}', str("\""+'date'+"\"")) \
        .replace('{link_forms}', str("\""+'link_forms'+"\"")) \
        .replace('{bom_dia_tarde_noite}', str("\""+'bom_dia_tarde_noite'+"\"")) \
        .replace('{concat_times}', str("\""+'concat_times'+"\""))\
        .replace('{argumento}', str("\""+'Que tal reagendar seu horário agora e economizar R$350,00 na avaliação que é cobrada por outras empresas? Basta clicar no botão abaixo!'+"\""))\
        .replace('{link_agendamento}', str("\""+'link_agendamento'+"\"")) 
    print(payload) 


    payload = json.loads(payload) 
    payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    #.encode('utf-8') 
    print('payload - ', payload)
    response = requests.post(URL_DIALOG_MESSAGE, data=payload, headers=HEADERS)  # Use HEADERS variable
    response_json = json.loads(response.text)
    print(response_json)

    enviar_wpp(telefone=telefone,
               msg = '------------------------------------------',
               header=HEADERS
               )

    messagebox.showinfo('Retorno do request', response_json)
  
def upload_media(media_type, media_path):
    header_upload_media = {
        'D360-Api-Key': f"{osFunc('API_DIALOG')}",
        'Content-Type': media_type,
    }
    

    with open(media_path, "rb") as media_file:
        media_data = media_file.read()

    try:
        response = requests.post("https://waba.360dialog.io/v1/media" , data=media_data, headers=header_upload_media)

        if response.status_code == 200 or response.status_code == 201:
            # Assuming the response contains JSON data with a key 'media_id'
            response_data = response.json()
            media_id = response_data['media'][0]['id']
            if media_id:
                print(f"Media uploaded successfully. Media ID: {media_id}")
                return media_id
            else:
                print("Media ID not found in the response.")
                return False
        else:
            print(f"Failed to upload media. Status code: {response.status_code}")
            print(response.text)  # Print response content for further debugging if needed
            return False

    except (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects) as e:
        print(e)
        return False

def uploadHSM(payload):
    URL_DIALOG_TEMPLATE = osFunc('URL_DIALOG_TEMPLATE')

    HEADERS = {
        'D360-Api-Key': osFunc('API_DIALOG'),
        'Content-Type': "application/json",
    }
    print(payload)
    payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    print(payload)
    response = requests.post(URL_DIALOG_TEMPLATE, data=payload, headers=HEADERS)
    response_json = json.loads(response.text) 
 
    return response_json

def HSM_text_transformation(texto):
    # encontrar todas as palavras que estão entre chaves
    palavras_entre_chaves = re.findall(r'\{([^}]+)\}', texto) 

    # encontrar os index de início e fim das palavras
    matches = re.finditer(r'\{([^}]+)\}', texto)
    begin_word = [match.start() + 1 for match in matches]
    end_word = [begin_word[i] + len(palavras_entre_chaves[i]) for i in range(len(palavras_entre_chaves))]

    # substituindo as palavras que dão match
    index = 0
    result = list(texto)
    variables = []
    diff = 0
    last_word = None
    for i, word in enumerate(palavras_entre_chaves): 
        variables.append(word)

        if word != last_word:    
            index += 1
        else: 
            variables.pop()

        begin = begin_word[i] - diff
        end = end_word[i] - diff

        result = result[:begin] + ["{" + str(index) + "}"] + result[end:] 
        
        diff += end - begin - 1
        last_word = word

    result = ''.join(result)

    return variables, result

def on_select(selected_options,buttons_value ,text_value, HSM_name, flow_bool, combo_flow_var,flow_cta):
    
    id_media = selected_options[1].get() 
    nomeHSM = HSM_name.get()
    tipologiaHSM = selected_options[0].get() 
    id_campanha = selected_options[2].get()
    type_media = selected_options[3].get() 
    buttonsValues = buttons_value.get('1.0', 'end-1c').split('\n')
    flow = flow_bool.get()
    nome_flow = combo_flow_var.get()
    flow_cta = flow_cta.get()
    print("Flow", nome_flow)
    name_variables, textValue = HSM_text_transformation(text_value.get('1.0', 'end-1c'))

    # textValue = json.dumps(text, ensure_ascii=False)
    conteudoSQL = json.dumps(text_value.get('1.0', 'end-1c'), ensure_ascii=False, indent=None)


    upload_payload = {"name": nomeHSM,"category": tipologiaHSM, "components": [{"type": "BODY","text": textValue}],"language": "pt_BR","allow_category_change": 'true'}
    
    SQL_payload =  {"to": "{telefone}",
                        "type": "template",
                        "messaging_product": "whatsapp",
                        "template": {
                            "namespace": "37e9fce0_4763_44a6_8294_f680359013ed",
                            "language": {"policy": "deterministic","code": "pt_BR"},
                            "name": nomeHSM}}


    # if id_media != '':
    #     link_video = 'videos/VID-20240920-WA0018.mp4'
    #     print(link_video)
        
    #     response_upload_video = upload_media("video/mp4", link_video)
        
    #     if not response_upload_video:
    #         messagebox.showerror('Retorno do upload de vídeo', "Erro no Upload do vídeo")
    #         return False
    #     else:
    #         id_media = response_upload_video
    #         print(id_media)

    if type_media == 'VIDEO':
        upload_payload["components"].append({"example": {"header_handle": [id_media]},"format": "VIDEO","type": "HEADER"})
        
        if not 'components' in SQL_payload["template"].keys():
            SQL_payload["template"]["components"] = []
        
        SQL_payload["template"]["components"].append({"type": "header", "parameters": [{"type": "video","video": {"link": id_media}}]})
        

    elif type_media == 'IMAGEM':

        upload_payload["components"].append({"example": {"header_handle": ['https://i.ibb.co/zH700br/cupom25.jpg']},"format": "IMAGE","type": "HEADER"})
        
        if not 'components' in SQL_payload["template"].keys():
            SQL_payload["template"]["components"] = []
        
        SQL_payload["template"]["components"].append({"type": "header", "parameters": [{"type": "image","image": {"link": id_media}}]})

    if len(name_variables) > 0: 
        upload_payload['components'][0]['example'] = {"body_text": [name_variables]} 

        list_variables = []
        for var in name_variables:
            list_variables.append({"type": "text", "text": "{"+ var +"}"})
        
        if not 'components' in SQL_payload["template"].keys():
            SQL_payload["template"]["components"] = []

        SQL_payload["template"]["components"].append({"type": "body",  "parameters": list_variables})
 

    if buttonsValues != ['']:
        buttons_list = []
        
        for value in buttonsValues:
            print(value)
            if value != '':
                buttons_list.append({"text": value, "type": "QUICK_REPLY"}) 
        if buttons_list:
            upload_payload["components"].append({"buttons": buttons_list,"type": "BUTTONS"})
 
    if flow:
        query_flows = f"""SELECT * FROM WHATSAPP_FLOWS WHERE NOME_FLOW = '{nome_flow}'"""
        print(query_flows)
        flow_df = query_to_dataframe(query_flows)
        id_flow = flow_df.iloc[0]['ID_FLOW']
        print("ID_FLOW", id_flow)
        flow_token = '"{telefone}_' + nome_flow
        flow_json = {"type":"BUTTONS","buttons":[{"type":"FLOW","text":f"{flow_cta}","flow_id":f"{id_flow}","flow_action":"data_exchange"}]}
        upload_payload["components"].append(flow_json)
        flow_component = {"type":"button","sub_type":"flow","index":"0","parameters":[{"type":"action","action":{"flow_token":f'{flow_token}'}}]}
        if not 'components' in SQL_payload["template"].keys():
            SQL_payload["template"]["components"] = []
        
        SQL_payload["template"]["components"].append(flow_component)
    APIresponse = uploadHSM(upload_payload)
 
    print(APIresponse)


    if "status" in APIresponse.keys():
    # if True:
        messagebox.showinfo('Retorno da função', "Upload realizado com sucesso!")

        SQL_payload = str(SQL_payload).replace("'{", "{").replace('"{', "{").replace('}"', "}").replace("}'","}").replace("'", '"')

        print(SQL_payload)
        timezone_brasilia = pytz.timezone("America/Sao_Paulo")
        datetime_brasilia = datetime.now(timezone_brasilia)
        datetime_formatado = datetime_brasilia.strftime("%Y-%m-%d %H:%M:%S")

        execute_query_db(f"""INSERT INTO HSM (ID_HSM, CONTEUDO, MESSAGE_PAYLOAD, ID_CAMPANHA, TIPOLOGIA, ATIVO, ATUALIZADO_EM,STATUS) VALUES
                                        ('{nomeHSM}', {conteudoSQL},'{SQL_payload}', '{id_campanha}', '{tipologiaHSM}', -1, '{datetime_formatado}', 'approved')""")
        
        
    else:
        messagebox.showerror('Retorno da função', APIresponse["meta"]["developer_message"])

def show_text_HSM(event, text_box, HSM_class):
    df = HSM_class.df
    text2show = df[df['ID_HSM'] == event.widget.get()].iloc[0]['CONTEUDO'] 
    text_box.delete("1.0", tk.END)
    text_box.insert("1.0",text2show)

def setup_HSM(frame):  

    row = 0
    label = tk.Label(frame, text= 'NOME DO HSM', font=('Arial', 11, 'bold'))
    label.grid(row=0, column=0, sticky=tk.W, padx=5)
    

    HSM_name = tk.Entry(frame)
    HSM_name.grid(row=0, column= 1, sticky=tk.W, padx=5)

    type_media = tk.StringVar(frame)
    # Opções para as listas suspensas
    variaveis = {
        'TIPOLOGIA': ['UTILITY', 'MARKETING'],
        'LINK': ['',
                'https://s3.sa-east-1.amazonaws.com/psd.hsm.sp/ia_leads_meta_15d_utility.mp4',
                'https://files.catbox.moe/r2gkup.jpg',],
        'CAMPANHA': list(range(-1, 50)),
        'TIPO DE MÍDIA': ['',
            'IMAGEM','VIDEO']
    }

    # Variáveis de controle para as listas suspensas
    selected_options = [tk.StringVar() for _ in range(len(variaveis))]

    for i, (name, options) in enumerate(variaveis.items()):
        row += 1
        label = tk.Label(frame, text=name)
        label.grid(row=row, column=0, sticky=tk.W, padx=5)
        
        combo = ttk.Combobox(frame, textvariable=selected_options[i], values=options, width= 30)
        combo.grid(row=row, column=1, sticky=tk.W, pady=5, padx=5)

        # Set the default value for each combobox
        combo.set(options[0])


    
    # row +=1
    # label = tk.Label(frame, text= 'URL DO VÍDEO') 
    # label.grid(row=row, column=0, sticky=tk.W, padx=5)

    # URL_video = tk.Text(frame, height=2, width=30)
    # URL_video.grid(row=row, column= 1, sticky=tk.W, padx=5)
    
    # button = tk.Button(frame, text="SELECIONAR URL", command=lambda: open_file_dialog(URL_video) )
    # button.grid(row= row, column=2, columnspan=2,padx=0 ,sticky=tk.W)  # Use grid manager for button
    
    row += 1
    flow_bool = tk.BooleanVar(value=False)  # Variável de controle da checkbox
    checkbox = tk.Checkbutton(frame, text="Habilitar Flow", variable=flow_bool)
    checkbox.grid(row=row, column=0, sticky=tk.W, padx=5)
    row += 1
    label = tk.Label(frame, text= 'CTA DO FLOW')
    label.grid(row=row, column=0, sticky=tk.W, padx=5)
    flow_cta = tk.Entry(frame)
    flow_cta.grid(row=row, column= 1, sticky=tk.W, padx=5)

    row += 1
    flow = Flow()
    label = tk.Label(frame, text= 'NOME DO FLOW')
    label.grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)
    variaveis_flows = list(flow.ID_FLOW_ativos_list)
    variaveis_flows.append('')
    combo_flow_var = tk.StringVar()
    combo = ttk.Combobox(frame, textvariable= combo_flow_var , values=variaveis_flows, width= 30)
    combo.grid(row=row, column=1, sticky=tk.W, pady=5, padx=5)
    combo.bind('<KeyRelease>', lambda event, values=variaveis_flows, combobox = combo: search(event, values,combobox))
    print("Combo", combo_flow_var.get())
    row +=1
    label = tk.Label(frame, text= 'BOTÕES (SEPARE POR LINHA)')
    label.grid(row=row, column=0, sticky=tk.W, padx=5)
    
    buttons = tk.Text(frame, height=3, width=30)
    buttons.grid(row=row, column= 1, sticky=tk.W, padx=5)

    row +=2

    label = tk.Label(frame, text= 'TEXTO DO HSM\n(adione o texto com as variáveis entre "{}" \nEx: {cliente}, {amigo}, {unidade_horario})')
    label.grid(row=row, column=0, columnspan=1, rowspan=2, sticky=tk.W, padx=5)
 
    text_container = tk.Frame(frame)
    text_container.grid(row=row, column=1, columnspan=2, sticky=tk.W, padx=5, pady=5)

    # Widget de texto com altura e largura especificadas

    text_widget = tk.Text(text_container, height=10, width=60)
    text_widget.pack(side='left', fill='both', expand=True)

    # Barra de rolagem
    scrollbar = tk.Scrollbar(text_container, command=text_widget.yview)
    scrollbar.pack(side='right', fill='y')

    # Configurar o widget de texto para usar a barra de rolagem
    text_widget.config(yscrollcommand=scrollbar.set)

    row +=1

    # Botão para obter seleções
    progress_frame = ttk.Frame(frame)
    progress_frame.grid(row=row, column=0, columnspan=2, rowspan=2,padx=10, pady=10)

    button = tk.Button(progress_frame, text="UPLOAD DO HSM", width=20 ,command= lambda: on_select(selected_options, 
                                                                              buttons, 
                                                                              text_widget, 
                                                                              HSM_name, flow_bool, combo_flow_var,flow_cta))

    button.grid(row= 0, column=0, pady=10)  # Use grid manager for button

def setup_teste_HSM(frame):
    HSM_class =  HSM() 

    frame1 = tk.Frame(frame)
    frame1.grid(row = 0)
    
    row = 1
    label = tk.Label(frame1, text= 'TELEFONE')
    label.grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)

    telefone = tk.Entry(frame1)
    telefone.grid(row=row, column= 1, sticky=tk.W, padx=5, pady=5)
 
    row +=1
    label = tk.Label(frame1, text= 'NOME DO HSM')
    label.grid(row=row, column=0, sticky=tk.W, padx=5, pady=5)

    variaveis = list(HSM_class.ID_HSM_ativos_list)

    combo_var = tk.StringVar()
    combo = ttk.Combobox(frame1, textvariable= combo_var , values=variaveis, width= 30)
    combo.grid(row=row, column=1, sticky=tk.W, pady=5, padx=5)
    combo.bind('<KeyRelease>', lambda event, values=variaveis, combobox = combo: search(event, values,combobox))

    row += 1
    button = tk.Button(frame1, text="ENVIAR HSM", width=20 ,command= lambda: enviar_HSM(combo_var, HSM_class.df,telefone))
    button.grid(row = row, column= 1)

    buttonAtualizar = tk.Button(frame1, text="ATUALIZAR", width=20 ,command= lambda: atualizar(combo, HSM_class))
    buttonAtualizar.grid(row = row, column= 0)
 

    HSM_text = tk.Text(frame, width=50)
    HSM_text.grid(row = 0, column= 1, rowspan= 5, sticky="nse") 

    combo.bind('<KeyRelease>', lambda event, values=variaveis, combobox = combo: search(event, values,combobox))
    combo.bind('<<ComboboxSelected>>', lambda event, HSM_text = HSM_text, HSM_class = HSM_class: show_text_HSM(event, HSM_text, HSM_class))

    

def hsm_status_page(frame):

    def deleteHSMfunction():
        # Get selected item IDs
        selected_items = tree.selection() 

        query = f'DELETE FROM HSM WHERE ID_HSM IN ('
        # Retrieve values for selected items
        i = 0
        for item_id in selected_items:
            values = tree.item(item_id, "values")
            if i>0: 
                query += ','
            query += "'" + str(values[0]) + "'" 
            i+=1
        query += ')'
        execute_query_db(query)
        update_treeview(HSM_class)

    def update_treeview(HSM_class):
        
        # Clear existing items in the Treeview
        for item in tree.get_children():
            tree.delete(item)

        HSM_class.atualizar()

        # Insert updated values into the Treeview
        for i, row in HSM_class.df.iterrows():         
            tree.insert("", tk.END, values=tuple(row.values), tags = (row['ATIVO']))

    def activateHSMfunction(code):
        # Get selected item IDs
        selected_items = tree.selection() 

        query = f'UPDATE HSM SET ATIVO = {code} WHERE ID_HSM IN ('
        # Retrieve values for selected items
        i = 0
        for item_id in selected_items:
            values = tree.item(item_id, "values")
            if i>0: 
                query += ','
            query += "'" + str(values[0]) + "'" 
            i+=1
        query += ')'
        execute_query_db(query)
        update_treeview(HSM_class)

    frame1 = tk.Frame(frame)
    frame1.grid(row = 0)

    HSM_class =  HSM() 
    dfHSM = HSM_class.df
    tree = ttk.Treeview(frame1, column=tuple(dfHSM.columns), show='headings')

    for i, column in enumerate(dfHSM.columns):
        tree.column(f"#{i + 1}", anchor=tk.CENTER)
        tree.heading(f"#{i + 1}", text=column)

    update_treeview(HSM_class)
    

    tree.tag_configure("400", background="#FFA592")
    tree.grid(row = 0, column=0, columnspan= 3 , pady = 10)

    # Button to trigger getting selected values
    activateHSM = tk.Button(frame, text="ATIVAR HSM", command=lambda:activateHSMfunction(200))
    activateHSM.grid(row = 1, column=0 , pady = 10)

    deactivateHSM = tk.Button(frame, text="DESATIVAR HSM", command=lambda:activateHSMfunction(-1))
    deactivateHSM.grid(row = 2, column=0 , pady = 10)

    deleteHSM = tk.Button(frame, text="DELETAR HSM", command=lambda:deleteHSMfunction())
    deleteHSM.grid(row = 3, column=0 , pady = 10)






    