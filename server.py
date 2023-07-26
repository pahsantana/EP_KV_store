import socket
import threading
import time
import json
from message import Message

class Server:
    def __init__(self, ip, port, leader_ip, leader_port):
        """
        Inicializa a classe Server com os seguintes parâmetros:

        Args:
            ip (str): O endereço IP do servidor.
            port (int): A porta em que o servidor irá escutar por conexões.
            leader_ip (str): O endereço IP do líder do conjunto de servidores.
            leader_port (int): A porta do líder do conjunto de servidores.

        Atributos:
            ip (str): O endereço IP do servidor.
            port (int): A porta em que o servidor irá escutar por conexões.
            leader_ip (str): O endereço IP do líder do conjunto de servidores.
            leader_port (int): A porta do líder do conjunto de servidores.
            defaultServers (list): Uma lista de tuplas contendo os endereços IP e portas dos servidores padrão.
            hashTable (dict): Um dicionário para armazenar as chaves e valores do servidor.
            lock (threading.Lock): Um objeto de bloqueio usado para evitar condições de corrida ao acessar a hashTable.
            socket (socket.socket): O socket do servidor para aceitar conexões.
        """
        self.ip = ip
        self.port = port
        self.leader_ip = leader_ip
        self.leader_port = leader_port
        self.defaultServers = [('127.0.0.1', 10097), ('127.0.0.1', 10098), ('127.0.0.1', 10099)]
        self.hashTable = {} 
        self.lock = threading.Lock()
        self.socket = None

        # Método para iniciar o servidor
    def start(self):
        # Criando o socket do servidor e o vinculando ao endereço IP e porta fornecidos
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, self.port))
        self.socket.listen()

        # Loop para aguardar conexões e tratar cada uma em uma nova thread
        while True:
            client_socket, _ = self.socket.accept()
            threading.Thread(target=self.handle_connection, args=(client_socket,)).start()

    # Método para processar uma requisição de replicação
    def replication_request(self, client_socket, message):
        # Adicionando a chave e valor recebidos à hashTable
        with self.lock:
            self.hashTable[message.key] = message.value

        # Exibindo informações sobre a replicação realizada
        print(f"REPLICATION key:{message.key} value:{message.value[0]} ts:{message.value[1]}")

        # Preparando e enviando a resposta de sucesso de replicação ao cliente
        response = Message("REPLICATION_OK", None, None)
        client_socket.sendall(json.dumps(response.to_json()).encode())

    # Método para processar uma requisição GET
    def get_request(self, client_socket, message):
        # Verificando se a chave existe na hashTable e obtendo o valor associado
        with self.lock:
            value = self.hashTable.get(message.key, '')

        # Verificando o valor e timestamp da mensagem recebida para formar a resposta adequada
        if value == '':
            response = Message("NULL", None, None)
        elif message.value[1] is not None and value[1] < message.value[1]:
            response = Message("TRY_OTHER_SERVER_OR_LATER", None, None)
        else:
            response = Message("GET_OK", message.key, value)

        # Convertendo a resposta para JSON e enviando-a ao cliente
        response_str = json.dumps(response.to_json())
        client_socket.sendall(response_str.encode())

        # Exibindo informações do GET realizado
        if response.request == "GET_OK":
            print(f"GET key: {response.key} value: {response.value[0]} obtido do servidor {client_socket.getpeername()}, meu timestamp {message.value[1]} e do servidor {response.value[1]}")

    # Método para processar uma requisição PUT
    def put_request(self, client_socket, message):
        if self.is_leader():
            # Caso seja o líder, processa a requisição PUT diretamente
            print(f"Cliente {client_socket.getpeername()} PUT key:{message.key} value:{message.value[0]}.")
            self.handle_put_leader(client_socket, message)
        else:
            # Caso não seja o líder, encaminha a requisição para o líder
            print(f"Encaminhando PUT key:{message.key} value:{message.value[0]}.")
            self.handle_put_refer_leader(client_socket, message)

    # Método para processar uma requisição PUT encaminhada pelo servidor não líder
    def handle_put_refer_leader(self, client_socket, message):
        try:
            # Criando um novo socket para se conectar ao servidor líder
            servers_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            servers_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
            servers_socket.connect((self.leader_ip, self.leader_port))

            # Enviando a requisição de PUT ao líder e aguardando a resposta
            servers_socket.sendall(json.dumps(message.to_json()).encode())
            request = servers_socket.recv(1024).decode()
            response = Message.from_json(request)

            # Enviando a resposta de sucesso ao cliente que solicitou a operação
            client_socket.sendall(json.dumps(Message("PUT_OK", response.key, response.value).to_json()).encode())
        except Exception as err:
            print(f'Erro ao encaminhar a requisição PUT por: {err}')

    # Método para processar uma requisição PUT recebida pelo servidor líder
    def handle_put_leader(self, client_socket, message):
        # Bloqueio para evitar condições de corrida ao acessar a hashTable
        with self.lock:
            # Adicionando o par chave-valor à hashTable com o timestamp atual
            self.hashTable[message.key] = ((message.value)[0], int(time.time()))

        # Lista para armazenar respostas de replicação dos outros servidores
        replication_responses = []
        for server in self.defaultServers:
            if server != (self.leader_ip, self.leader_port):
                try:
                    # Criando um novo socket para se conectar ao servidor para replicação
                    leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    leader_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
                    leader_socket.connect(server)

                    # Enviando a requisição de replicação ao servidor            
                    time.sleep(30) # Aguardando um intervalo de tempo (30 segundos) para replicar como padrão
                    leader_socket.sendall(json.dumps(Message("REPLICATION", message.key, ((message.value)[0], int(time.time()))).to_json()).encode())
                    response = leader_socket.recv(1024).decode()
                    replication_responses.append(json.loads(response))
                    leader_socket.close()
                except Exception as err:
                    print(f'Erro ao replicar para o servidor {server[0]}:{server[1]}: {err}')

        # Verificando se todas as respostas de replicação são "REPLICATION_OK"
        if all(response.get('request') == 'REPLICATION_OK' for response in replication_responses):
            # Enviando a resposta "PUT_OK" ao cliente que solicitou a operação
            print(f"Enviando PUT_OK ao Cliente {client_socket.getpeername()} da key:{message.key} ts:{int(time.time())}.")
            client_socket.sendall(json.dumps(Message("PUT_OK", message.key, ((message.value)[0],int(time.time()))).to_json()).encode())
        else:
            # Caso haja alguma falha na replicação em algum dos servidores
            print("Falha na replicação em algum dos servidores")

    # Método para processar uma conexão do cliente com o servidor
    def handle_connection(self, client_socket):
        try:
            # Recebendo a requisição do cliente
            request = client_socket.recv(1024).decode()
            message = Message.from_json(request)

            # Verificando o tipo de requisição e tratando-a em uma nova thread
            if message:
                if message.request == 'PUT':
                    threading.Thread(target=self.put_request, args=(client_socket, message)).start()
                elif message.request == 'REPLICATION':
                    threading.Thread(target=self.replication_request, args=(client_socket, message)).start()
                elif message.request == 'GET':
                    threading.Thread(target=self.get_request, args=(client_socket, message)).start()
                else:
                    # Caso a requisição seja inválida
                    print('INVALID_OPTION')
        except json.JSONDecodeError as json_err:
            print(f'Erro de decodificação JSON: {json_err}')
        except Exception as err:
            print(f'Erro de conexão: {err}')

    # Método para verificar se o servidor é o líder
    def is_leader(self):
        return self.ip == self.leader_ip and self.port == self.leader_port

# Verifica se o script é o arquivo principal
if __name__ == "__main__":
    # Solicita o endereço IP, porta e informações do líder do servidor ao usuário
    ip = input("Digite o endereço IP do servidor (padrão: 127.0.0.1): ").strip() or '127.0.0.1'
    port = int(input("Digite a porta do servidor (padrão: 10097, 10098, 10099): ").strip())
    leader_ip = input("Digite o endereço IP do líder (padrão: 127.0.0.1): ").strip() or '127.0.0.1'
    leader_port = int(input("Digite a porta do líder: (padrão: 10097, 10098, 10099):").strip())

    # Cria e inicia o servidor com as informações fornecidas
    server = Server(ip, port, leader_ip, leader_port)
    server.start()