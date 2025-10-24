import google.generativeai as genai

# вставь сюда свой ключ
genai.configure(api_key="AIzaSyDYA3d0EPVNUrRtQW0mAmQ_T8z6XDVJZr4")

print("📋 Список доступных моделей:\n")
for m in genai.list_models():
    if "generateContent" in m.supported_generation_methods:
        print("-", m.name)
