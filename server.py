import asyncio
import socket
import json
import os
import threading
from datetime import datetime

# --- Конфигурация сервера ---
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 12345
CONFIG_FILE = 'client_ips.json'
SERVER_FOLDER = 'server_data'  # Папка сервера
CHUNK_SIZE = 65535  # Размер чанка для потоковой передачи

# --- Блокировка для доступа к файлам ---
file_lock = threading.Lock()

GET = b'\x01'
PUT = b'\x02'
DELETE = b'\x03'
LIST = b'\x04'
RESPONSE = b'\x05'

FileNotFound = b'\x01'
ConnectionBreak = b'\x02'
СhecksumError = b'\x03'

FLAG_ERROR = b'\x80'
FLAG_SIZE = b"\x40"
FLAG_STATUS = b'\x20'

STATUS_OK = b"\x80"
STATUS_NOTOK = b"\x00"

# --- Функции сервера ---
def load_client_ips():
    """Загружает список IP-адресов клиентов из конфигурационного файла."""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_client_ips(client_ips):
    """Сохраняет список IP-адресов клиентов в конфигурационный файл."""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(client_ips, f, indent=4)


async def send_file(loop, client_socket, filepath):
    """Отправляет файл по частям."""
    try:
        with open(filepath, 'rb') as file:
            bytes_sent = 0
            while True:
                chunk = file.read(CHUNK_SIZE)  # Читаем кусок файла
                if not chunk:
                    break  # Конец файла
                await loop.sock_sendall(client_socket, chunk)
                bytes_sent += len(chunk)
                print(f"Сервер: Отправлен кусок ({len(chunk)} байт), всего отправлено {bytes_sent} байт")
        print(f"Сервер: Файл '{os.path.basename(filepath)}' успешно отправлен.")
        return True
    except FileNotFoundError:
        print("Сервер: Файл не найден")
        return False
    except Exception as e:
        print(f"Сервер: Ошибка при отправке файла: {e}")
        return False


async def receive_file(loop, client_socket, filepath, file_size):
    """Получает файл по частям."""
    try:
        with open(filepath, 'wb') as file:
            bytes_received = 0
            while bytes_received < file_size:
                chunk = await loop.sock_recv(client_socket, CHUNK_SIZE)
                if not chunk:
                    print("Сервер: Соединение с клиентом прервано во время передачи.")
                    return False  # Соединение прервано
                file.write(chunk)
                bytes_received += len(chunk)
                print(f"Сервер: Получен кусок ({len(chunk)} байт), всего получено {bytes_received} байт из {file_size}")
        print(f"Сервер: Файл '{os.path.basename(filepath)}' успешно получен.")
        return True  # Файл успешно получен
    except Exception as e:
        print(f"Сервер: Ошибка при получении файла: {e}")
        return False


async def update_request_count(client_ip):
    client_ips = load_client_ips()
    client_ips[client_ip]["request_count"] += 1
    save_client_ips(client_ips)


async def update_occupied_space(client_ip, file_size):
    client_ips = load_client_ips()
    if client_ips[client_ip]["occupied_space"] + file_size < client_ips[client_ip]["quote"]:
        client_ips[client_ip]["occupied_space"] += file_size
        save_client_ips(client_ips)
        return True
    else:
        return False


async def delete_occupied_space(client_ip, file_size):
    client_ips = load_client_ips()
    client_ips[client_ip]["occupied_space"] -= file_size
    save_client_ips(client_ips)


async def handle_client_request(loop, client_socket, client_address, client_ips):
    """Обрабатывает запросы от клиента асинхронно."""
    client_ip = client_address[0]

    try:
        await loop.sock_sendall(client_socket, RESPONSE + FLAG_STATUS + STATUS_OK)
        print(f"Сервер: Попытка подключения принята: {client_address}")
        data = await loop.sock_recv(client_socket, 1024)  # Получаем запрос от клиента
        if not data:
            print(f"Сервер: Клиент {client_address} отключился.")
            return

        command = data[:1]
        filename = ''

        if command == GET:
            filename_size = int.from_bytes(data[1:3], byteorder='big')
            filename = data[3:].decode('utf-8')
            # print(data)
            # print(filename_size)
            # print(filename)
            try:
                filepath = os.path.join(SERVER_FOLDER + f"/{client_ip}", filename)
                file_size = os.path.getsize(filepath)
                print(f"Сервер: Размер запрашиваемого файла: {file_size}")
                await loop.sock_sendall(client_socket, RESPONSE + FLAG_SIZE + file_size.to_bytes(4, "big"))  # Отправляем размер файла
                # print(RESPONSE, FLAG_SIZE == b'\x40', file_size.to_bytes(4, "big"))
                response = await loop.sock_recv(client_socket, 1024)  # Ждем подтверждения от клиента (READY)
                # print(response)
                if response[:1] == RESPONSE and response[1:2] == FLAG_STATUS and response[2:] == STATUS_OK:
                    await send_file(loop, client_socket, filepath)  # Отправляем файл потоком
                    print(f"Сервер: Отправлен файл '{filename}' клиенту {client_address}, размер: {file_size} байт.")
                    response = await loop.sock_recv(client_socket, 1024)
                    if response[:1] == RESPONSE and response[1:2] == FLAG_STATUS and response[2:] == STATUS_OK:
                        print("Сервер: Клиент принял файл")
                    else:
                        print("Сервер: Ошибка принятия файла у клиента")
                else:
                    print("Сервер: Клиент не готов к приему файла")

            except FileNotFoundError:
                await loop.sock_sendall(client_socket, RESPONSE + FLAG_ERROR + FileNotFound)
            except Exception as e:
                await loop.sock_sendall(client_socket, RESPONSE + FLAG_ERROR + ConnectionBreak)

            await update_request_count(client_ip)
        elif command == PUT:
            filename_size = int.from_bytes(data[1:3], byteorder='big')
            filename = data[3:].decode('utf-8')

            try:
                response = await loop.sock_recv(client_socket, 1024)  # Получаем размер файла от клиента
                if response[:1] == RESPONSE and response[1:2] == FLAG_SIZE:
                    file_size = int.from_bytes(response[2:], 'big')
                else:
                    if response[:1] == RESPONSE and response[1:2] == FLAG_ERROR and response[2:] == FileNotFound:
                        print(f"Клиент: Ошибка: файл не найден")
                    else:
                        print(f"Клиент: Непредвиденная ошибка")
                filepath = os.path.join(SERVER_FOLDER + f"/{client_ip}", filename)
                print(f"Сервер: Получен размер файла от клиента: {file_size}")
                response = await update_occupied_space(client_ip, file_size)
                if response:
                    await loop.sock_sendall(client_socket, RESPONSE + FLAG_STATUS + STATUS_OK)  # Подтверждаем готовность к приему
                else:
                    await loop.sock_sendall(client_socket, RESPONSE + FLAG_STATUS + STATUS_NOTOK)

                success = await receive_file(loop, client_socket, filepath, file_size)
                if not success:
                    print("Сервер: Ошибка при получении файла.")
                    await loop.sock_sendall(client_socket, RESPONSE + FLAG_STATUS + STATUS_NOTOK)
                else:
                    await loop.sock_sendall(client_socket, RESPONSE + FLAG_STATUS + STATUS_OK)

            except Exception as e:
                print(f"Сервер: Ошибка: {e}")
                await loop.sock_sendall(client_socket, RESPONSE + FLAG_ERROR + FileNotFound)

            await update_request_count(client_ip)

        elif command == LIST:
            try:
                with file_lock:  # Получаем блокировку перед доступом к файлу
                    files_info = []
                    result = b""
                    for filename in os.listdir(SERVER_FOLDER + f"/{client_ip}"):
                        filepath = os.path.join(SERVER_FOLDER + f"/{client_ip}", filename)
                        if os.path.isfile(filepath):
                            file_size = os.path.getsize(filepath)
                            result += len(filename).to_bytes(2, "big") + filename.encode("utf-8") + file_size.to_bytes(4, "big")
                            # print(result)
                await loop.sock_sendall(client_socket, result)
            except Exception as e:
                await loop.sock_sendall(client_socket, RESPONSE + FLAG_ERROR + ConnectionBreak)

            await update_request_count(client_ip)

        elif command == DELETE:
            filename_size = int.from_bytes(data[1:3], byteorder='big')
            filename = data[3:].decode('utf-8')
            try:
                filepath = os.path.join(SERVER_FOLDER + f"/{client_ip}", filename)
                file_size = os.path.getsize(filepath)
                with file_lock:
                    os.remove(filepath)
                await loop.sock_sendall(client_socket, RESPONSE + FLAG_STATUS + STATUS_OK)
                await delete_occupied_space(client_ip, file_size)
                print(f"Сервер: Файл '{filename}' удален")
            except FileNotFoundError:
                await loop.sock_sendall(client_socket, RESPONSE + FLAG_ERROR + FileNotFound)
            except Exception as e:
                await loop.sock_sendall(client_socket, RESPONSE + FLAG_ERROR + ConnectionBreak)

            await update_request_count(client_ip)
        else:
            await loop.sock_sendall(client_socket, "ERROR Invalid command".encode('utf-8'))

    except ConnectionResetError:
        print(f"Сервер: Клиент {client_address} неожиданно отключился.")
    except Exception as e:
        print(f"Сервер: Ошибка при обработке запроса клиента {client_address}: {e}")
    finally:
        client_socket.close()


async def forbidden(loop, client_socket, client_address):
    await loop.sock_sendall(client_socket, RESPONSE + FLAG_STATUS + STATUS_NOTOK)
    print(f"Сервер: Попытка подключения отклонена: {client_address}")


async def run_server():
    """Запускает асинхронный сервер."""
    # Создаем папку сервера, если она не существует
    if not os.path.exists(SERVER_FOLDER):
        os.makedirs(SERVER_FOLDER)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((SERVER_HOST, SERVER_PORT))
    server_socket.listen(100)
    server_socket.setblocking(False)  # Важно для асинхронной работы

    loop = asyncio.get_running_loop()

    print(f"Сервер слушает на {SERVER_HOST}:{SERVER_PORT}")

    try:
        while True:
            client_socket, client_address = await loop.sock_accept(server_socket)  # Асинхронный accept
            print(f"Сервер: Попытка подключиться: {client_address}")

            client_ip = client_address[0]
            client_ips = load_client_ips()

            if client_ip not in client_ips:
                client_ips[client_ip] = {"first_seen": str(datetime.now()),
                                         "block": False,
                                         "quote": 1526260,
                                         "occupied_space": 0,
                                         "request_count": 0
                                         }
                save_client_ips(client_ips)
                try:
                    new_directory_path = os.path.join(SERVER_FOLDER, client_ip)
                    os.makedirs(new_directory_path, exist_ok=False)  # Создаем директорию
                    await loop.sock_sendall(client_socket, "OK Directory created".encode('utf-8'))
                    print(f"Сервер: Создана директория '{client_ip}'")
                except FileExistsError:
                    await loop.sock_sendall(client_socket, "ERROR Directory already exists".encode('utf-8'))
                except Exception as e:
                    await loop.sock_sendall(client_socket, f"ERROR {str(e)}".encode('utf-8'))
                print(f"Сервер: Новый клиент {client_ip} добавлен в конфигурацию.")
            if client_ips[client_ip]["block"] is False:
                loop.create_task(handle_client_request(loop, client_socket, client_address, client_ips))  # Запускаем обработку клиента
            else:
                loop.create_task(forbidden(loop, client_socket, client_address))
    finally:
        server_socket.close()


if __name__ == "__main__":
    import asyncio
    from datetime import datetime

    asyncio.run(run_server())