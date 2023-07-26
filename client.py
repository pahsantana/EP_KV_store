import socket
import random
import json
import time
from message import Message

class Client:
    def __init__(self):
        """
        Inicializa a classe Client.

        Atributos:
            servers (list): Uma lista de tuplas para armazenar os endereços IP e portas dos servidores.
            hashTable (dict): Um dicionário para armazenar chaves e valores obtidos pelo cliente.
        """
        self.servers = None
        self.hashTable = {}

    def initialize_servers(self):
        """
        Inicializa os endereços IP e portas dos servidores.

        Esse método captura do usuário os endereços IP e portas dos três servidores disponíveis
        e armazena-os na lista de servidores 'servers'.
        """
        # Capturar os IPs e portas dos três servidores
        self.servers = []
        for i in range(3):
            server_ip = input(f"Digite o endereço IP do servidor {i+1} (padrão: 127.0.0.1): ").strip() or '127.0.0.1'
            server_port = int(input(f"Digite a porta do servidor {i+1} (padrão: {10097 + i}): ").strip() or 10097 + i)
            self.servers.append((server_ip, server_port))

    def put_request(self):
        """
        Realiza uma requisição PUT em um servidor aleatório.

        O método solicita ao usuário uma chave e valor para serem enviados ao servidor.
        Em seguida, escolhe aleatoriamente um servidor da lista de servidores 'servers'
        e cria uma mensagem de requisição PUT com a chave e valor informados. A mensagem é
        enviada para o servidor e o cliente aguarda a resposta. Se a resposta for PUT_OK,
        indica que a operação foi realizada com sucesso e a chave e valor são adicionados ao hashTable.
        """
        # Verifica se os servidores foram inicializados
        if self.servers is None:
            raise ValueError("Array de servers não foi inicializado")

        # Solicita chave e valor do usuário
        key = input("Entre a chave desejada: ")
        value = input("Entre o valor desejado: ")

        # Escolhe aleatoriamente um servidor para realizar a requisição PUT
        selected_server = random.choice(self.servers)

        # Cria a mensagem de requisição PUT
        message = Message("PUT", key, (value, time.time()))

        try:
            # Cria e conecta o socket do cliente ao servidor selecionado
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect(selected_server)

                # Envia a mensagem de requisição PUT ao servidor
                client_socket.sendall(json.dumps(message.to_json()).encode())

                # Recebe a resposta do servidor
                request = client_socket.recv(1024).decode()
                response = Message.from_json(request)

                # Verifica a resposta do servidor
                if response.request == "PUT_OK":
                    # Se a resposta for PUT_OK, indica que a operação foi bem-sucedida
                    # não descobre o servidor líder, apenas exibe o escolhido randomicamente
                    print(f"PUT_OK key: {response.key} value {response.value[0]} timestamp {response.value[1]} realizada pelo servidor {selected_server[0]}:{selected_server[1]}")

                    # Atualiza o hashTable com a chave e o valor obtidos
                    self.hashTable[key] = response.value
        except Exception as err:
            # Trata exceções caso ocorra algum erro na requisição PUT
            print(f"Erro ao solicitar a requisição PUT por: {err}")

    def get_request(self):
        """
        Realiza uma requisição GET em um servidor aleatório.

        O método solicita ao usuário uma chave e verifica se ela existe no hashTable local.
        Caso exista, obtém o valor e o último timestamp associados à chave. Em seguida,
        escolhe aleatoriamente um servidor da lista de servidores 'servers' e cria uma mensagem
        de requisição GET com a chave e o último timestamp conhecido. A mensagem é enviada para
        o servidor e o cliente aguarda a resposta. Dependendo da resposta, imprime na tela a
        informação obtida ou uma mensagem indicando que a chave não foi encontrada ou que é
        necessário tentar outro servidor ou mais tarde.
        """
        # Verifica se os servidores foram inicializados
        if self.servers is None:
            raise ValueError("Array de servers não foi inicializado")

        # Solicita a chave ao usuário
        key = input("Entre a chave desejada: ")

        # Escolhe aleatoriamente um servidor para realizar a requisição GET
        selected_server = random.choice(self.servers)

        # Verifica se a chave existe no hashTable local e obtém o valor e o timestamp associados
        # Caso não exista, define value como None e timestamp como None
        value_and_timestamp = self.hashTable.get(key)
        if value_and_timestamp is not None:
            value, timestamp = value_and_timestamp
        else:
            value, timestamp = None, 0

        # Cria a mensagem de requisição GET com a chave e o último timestamp conhecido
        message = Message("GET", key, (value, timestamp))

        try:
            # Cria e conecta o socket do cliente ao servidor selecionado
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect(selected_server)

                # Envia a mensagem de requisição GET ao servidor
                client_socket.sendall(json.dumps(message.to_json()).encode())

                # Recebe a resposta do servidor
                response = client_socket.recv(1024).decode()
                response = Message.from_json(response)

                # Verifica a resposta do servidor
                if response.request == "NULL":
                    # Se a resposta for NULL, a chave não foi encontrada no servidor
                    print("Chave não encontrada")
                elif response.request == "TRY_OTHER_SERVER_OR_LATER":
                    # Se a resposta for TRY_OTHER_SERVER_OR_LATER, é necessário tentar outro servidor ou mais tarde
                    print("Tente outro servidor ou mais tarde")
                else:
                    # Se a resposta for GET_OK, imprime o valor obtido do servidor
                    # não descobre o servidor líder, apenas exibe o escolhido randomicamente
                    print(f"GET key: {response.key} value: {response.value[0]} obtido do servidor {selected_server[0]}:{selected_server[1]}, meu timestamp {int(time.time())} e do servidor {response.value[1]}")
        except Exception as err:
            # Trata exceções caso ocorra algum erro na requisição GET
            print(f"Erro ao solicitar a requisição GET por: {err}")


    def menu(self):
        """
        Exibe um menu de opções para o usuário interagir com o cliente.

        O usuário pode escolher entre as opções de inicializar os servidores, realizar uma requisição PUT
        ou realizar uma requisição GET. O menu é executado em loop até que o usuário escolha sair.
        """
        while True:
            print("---- MENU ----")
            print("0. INIT")
            print("1. PUT")
            print("2. GET")

            choice = input("Escolha um número dentro das opções: ")

            if choice == "0":
                self.initialize_servers()
            elif choice == "1":
                self.put_request()
            elif choice == "2":
                self.get_request()
            else:
                print("Opção inválida")

if __name__ == "__main__":
    client = Client()
    client.menu()
