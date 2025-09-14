import time
import win32com.client
import speech_recognition as sr
from fuzzywuzzy import fuzz

# Initialize TTS (Text-to-Speech)
speaker = win32com.client.Dispatch("SAPI.SpVoice")
voices = speaker.GetVoices()
speaker.Voice = voices.Item(0)  # Use David or your preferred voice

# Speak Function
def speak(text):
    print(f"\n🗣️ Jarvis: {text}")
    speaker.Speak(text)

# Greeting on start
speak("Systems online sir. Welcome back")

# Main Assistant Loop
while True:
    recognizer = sr.Recognizer()
    with sr.Microphone() as source:
        print("\n🔁 Jarvis is standing by. Say 'Hey Jarvis' to activate...\n")
        recognizer.adjust_for_ambient_noise(source, duration=1)
        audio = recognizer.listen(source)

    try:
        command = recognizer.recognize_google(audio)
        print(f"🎧 You said: {command}")

        # Wake word detected
        if fuzz.ratio(command.lower(), "hey jarvis") > 80:
            speak("Yes sir, command mode online.")

            # Enter command mode loop
            while True:
                with sr.Microphone() as source:
                    recognizer.adjust_for_ambient_noise(source, duration=1)
                    print("🎤 Listening for your command...")
                    audio = recognizer.listen(source)

                try:
                    task = recognizer.recognize_google(audio)
                    print(f"📥 Command received: {task}")

                    task_lower = task.lower()

                    if "time" in task_lower:
                        current_time = time.strftime("%I:%M %p")
                        speak(f"The time is {current_time}")

                    elif "sleep jarvis" in task_lower:
                        speak("Going offline sir.")
                        exit()

                    elif "exit" in task_lower or "thank you" in task_lower:
                        speak("Exiting command mode, sir.")
                        break

                    else:
                        speak("Sorry sir, I didn't understand that command.")

                except sr.UnknownValueError:
                    speak("I couldn't understand that.")
                except sr.RequestError:
                    speak("There was a network issue.")

        # Optional: Handle direct shutdown outside wake word
        elif fuzz.ratio(command.lower(), "sleep jarvis") > 80:
            speak("Going offline sir.")
            break

    except sr.UnknownValueError:
        print("Didn't catch that.")
    except sr.RequestError:
        print("Could not connect to Google API.")
