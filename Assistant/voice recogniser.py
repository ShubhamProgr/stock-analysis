import speech_recognition as sr

# analyze audio into texts (kind of a engline)
recognizer = sr.Recognizer()

# opens microphone to listen
with sr.Microphone() as source:
    print("Listening...")
    recognizer.adjust_for_ambient_noise(source)  # optional: cancel background noise
    audio = recognizer.listen(source) #tells the recognizer to hear until it hears a pause

try:
    # Convert audio to text and stores in variable "command"
    command = recognizer.recognize_google(audio)
    print(f" You said: {command}")

except sr.UnknownValueError:
    print("Sorry, I could not understand the audio.")
except sr.RequestError:
    print("Could not request results; check your internet.")