import win32com.client

speaker = win32com.client.Dispatch("SAPI.SpVoice")

voices = speaker.GetVoices()
speaker.Voice = voices.Item(0)
speaker.Speak("Systems online sir")
