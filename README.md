.\nats-server.exe

uvicorn main:app

.\nats.exe sub "currency.updates"

.\nats.exe pub currency.updates '{"code":"FAKE","rate":999.9,"name":"Test_Currency"}'

ws://127.0.0.1:8000/ws/items