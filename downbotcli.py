import os
import requests
import magic
import py7zr
import urllib
from argparse import Namespace
from urllib.parse import urlparse, unquote
from deltachat2 import Bot, ChatType, CoreEvent, EventType, MsgData, NewMsgEvent, events, AttrDict
from deltabot_cli import BotCli

# Tamaño máximo permitido (300 MB)
MAX_FILE_SIZE = 300 * 1024 * 1024

HELP = """Bot para descargar archivos mediante Deltachat impulsado por deltabot-cli:
  https://github.com/deltachat-bot/deltabot-cli-py
"""

cli = BotCli("file_splitter_bot")

def validate_url(url):
    """
    Valida que la URL tenga un esquema (http/https) y un dominio.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ['http', 'https']:
            return False
        if not parsed.netloc:
            return False
        return True
    except Exception:
        return False

def get_url_file_name(url, response):
    try:
        if "Content-Disposition" in response.headers:
            name = response.headers["Content-Disposition"]
            name = name.replace('attachment; ', '')
            name = name.replace('filename=', '').replace('"', '')
            name = name.replace("inline;", "").lstrip()
            name = name.replace(' ', '_')
            return name
        else:
            url_fix = unquote(url, encoding='utf-8', errors='replace')
            tokens = url_fix.split('/')
            name = tokens[-1]
            name = name.replace("inline;", "").lstrip().replace(' ', '_')
            return name
    except:
        url_fix = unquote(url, encoding='utf-8', errors='replace')
        tokens = url_fix.split('/')
        name = tokens[-1]
        name = name.replace("inline;", "").lstrip().replace(' ', '_')
        return name

def download_file(url, dest_path):
    """
    Descarga el archivo de la URL dada y lo guarda en dest_path.
    Se utiliza stream para manejar archivos grandes y se levanta excepción
    si ocurre algún error en la descarga.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Levanta un excepción para errores HTTP
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return response

def get_file_mime_extension(file_path):
    """
    Detecta el type MIME del archivo y retorna la extensión derivada.
    """
    mime_obj = magic.Magic(mime=True)
    file_mime = mime_obj.from_file(file_path)
    extension = file_mime.split('/')[1]
    return extension

def compress_and_split_file(file_path, temp_dir, part_size):
    """
    Comprime el archivo en formato 7z y lo divide en partes del tamaño especificado.
    Retorna una lista con la ruta de cada parte.
    """
    archive_path = os.path.join(temp_dir, "archive.7z")
    with py7zr.SevenZipFile(archive_path, 'w') as archive:
        archive.writeall(file_path, arcname=os.path.basename(file_path))

    parts = []
    part_num = 1
    with open(archive_path, 'rb') as f:
        while True:
            chunk = f.read(part_size)
            if not chunk:
                break
            part_file_name = f"{os.path.basename(file_path)}.7z.{part_num:04d}"
            part_file_path = os.path.join(temp_dir, part_file_name)
            with open(part_file_path, 'wb') as part_file:
                part_file.write(chunk)
            parts.append(part_file_path)
            part_num += 1
    return parts

def cleanup_temp_dir(temp_dir):
    """
    Elimina el directorio temporal y su contenido.
    """
    for root, dirs, files in os.walk(temp_dir, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(temp_dir)

@cli.on_init
def on_init(bot: Bot, args: Namespace) -> None:
    for accid in bot.rpc.get_all_account_ids():
       bot.rpc.set_config(accid, "displayname", "DownBot")
       bot.rpc.set_config(accid, "selfstatus", HELP)
       bot.rpc.set_config(accid, "delete_device_after", str(60 * 60 * 24))


@cli.on(events.RawEvent)
def log_event(bot, accid, event):
    bot.logger.info(f"Event: {event}")

@cli.on(events.NewMessage)
def handle_message(bot, accid, event):
    msg = event.msg
    bot.logger.info(f"Nuevo mensaje recibido: {msg.text}")

    if msg.text.strip() == "/help":
        help_message = (
            "Modo de uso de DownBot:\n\n"

            "1. Envía un mensaje con la URL y el tamaño de parte deseado en MB, ejemplo:\n"
            "   `http://example.com/file.zip 5`\n\n"

            "2. Si no se especifica el tamaño, se usará 10 MB por defecto.\n\n"

            "3. El tamaño máximo permitido es 300 MB."
        )
        bot.rpc.send_msg(accid, msg.chat_id, MsgData(text=help_message))
        return

    # Separar parámetros del mensaje
    parts = msg.text.split(' ')
    if len(parts) < 1:
        bot.rpc.send_msg(accid, msg.chat_id, MsgData(text="Formato incorrecto. Usa /help para ver instrucciones."))
        return

    url = parts[0].strip()
    if not validate_url(url):
        bot.rpc.send_msg(accid, msg.chat_id, MsgData(text="La URL proporcionada no es válida."))
        return

    try:
        part_size = int(parts[1]) * 1024 * 1024 if len(parts) == 2 else 10 * 1024 * 1024
    except Exception:
        bot.rpc.send_msg(accid, msg.chat_id, MsgData(text="El tamaño de parte debe ser un número válido en MB."))
        return

    # Indicar que se está procesando el mensaje
    bot.rpc.send_reaction(accid, msg.id, ["Downloading.."])

    # Crear un directorio temporal
    temp_dir = "temp_files"
    os.makedirs(temp_dir, exist_ok=True)

    try:
        # Descargar el archivo
        dest_file = os.path.join(temp_dir, "downloaded_file")
        response = download_file(url, dest_file)
        file_name = get_url_file_name(url, response)
        file_path = os.path.join(temp_dir, file_name)
        os.rename(dest_file, file_path)

        # Ajustar extensión del archivo según su MIME type
        extension = get_file_mime_extension(file_path)
        if not file_path.endswith(f".{extension}"):
            new_file_path = f"{file_path}.{extension}"
            os.rename(file_path, new_file_path)
            file_path = new_file_path

        file_size = os.path.getsize(file_path)
        bot.logger.info(f"Tamaño del archivo: {file_size} bytes")

        # Comprobar el límite de 300 MB
        if file_size > MAX_FILE_SIZE:
            bot.rpc.send_msg(accid, msg.chat_id, MsgData(text="El archivo excede el tamaño máximo permitido de 300 MB."))
            bot.rpc.send_reaction(accid, msg.id, [""])
            cleanup_temp_dir(temp_dir)
            return

        # Enviar el archivo directamente o comprimir y dividir según su tamaño
        if file_size <= part_size:
            bot.logger.info("Enviando archivo sin comprimir, ya que es menor o igual al tamaño especificado")
            bot.rpc.send_msg(accid, msg.chat_id, MsgData(file=file_path))
        else:
            parts_list = compress_and_split_file(file_path, temp_dir, part_size)
            for idx, part in enumerate(parts_list, start=1):
                bot.logger.info(f"Enviando parte {idx} a chat {msg.chat_id}")
                bot.rpc.send_msg(accid, msg.chat_id, MsgData(file=part))

        bot.rpc.send_reaction(accid, msg.id, [""])
    except requests.HTTPError as http_err:
        bot.logger.error(f"Error de descarga: {http_err}")
        bot.rpc.send_msg(accid, msg.chat_id, MsgData(text="Error al descargar el archivo. Verifica la URL."))
        bot.rpc.send_reaction(accid, msg.id, [""])
    except Exception as e:
        bot.logger.error(f"Error al procesar el mensaje: {e}")
        bot.rpc.send_msg(accid, msg.chat_id, MsgData(text="Ocurrió un error al procesar el archivo."))
        bot.rpc.send_reaction(accid, msg.id, [""])
    finally:
        cleanup_temp_dir(temp_dir)

if __name__ == "__main__":
    cli.start()