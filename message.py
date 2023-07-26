import json

class Message:
    def __init__(self, request, key, value):
        self.request = request
        self.key = key
        self.value = value

    #Mapper
    def to_json(self):
        return {
            "request": self.request,
            "key": self.key,
            "value": self.value,
        }

    @classmethod #static method
    def from_json(cls, json_received):
        try:
            data = json.loads(json_received)
            return cls(data["request"], data["key"], data["value"])
        except json.JSONDecodeError as json_err:
            print(f'Erro de decodificação JSON: {json_err}')