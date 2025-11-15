import google.generativeai as genai

# Замените на свой ключ
API_KEY = "AIzaSyDYA3d0EPVNUrRtQW0mAmQ_T8z6XDVJZr4"

def main():
    genai.configure(api_key=API_KEY)

    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content("ping")
        print("Ответ модели:", response.text)
        print("\n✓ Ключ работает, лимит не исчерпан.")
    except Exception as e:
        print("Ошибка при вызове API:")
        print(e)
        print("\n✗ Возможно, достигнут лимит или ключ некорректен.")

if __name__ == "__main__":
    main()
