import os
import subprocess
import logging
import time
import requests
import json
import edge_tts
import asyncio
import telebot
from telebot import types
from flask import Flask, request, abort

TELEGRAM_TOKEN = "8409832972:AAGLcBs7q6PwtxZDGpB-3SCNgTwzfPKPUVw"
ASSEMBLYAI_KEY = "a356bbda79da4fd8a77a12ad819c47e2"
GEMINI_KEY = "AIzaSyB1HVBY1a8XGE3bijTNJVBO1W759yK5KGc"
WEBHOOK_URL = "https://midkayga-4-raad.onrender.com"

FFMPEG_ENV = os.environ.get("FFMPEG_BINARY", "")
POSSIBLE_FFMPEG_PATHS = [FFMPEG_ENV, "./ffmpeg", "/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg", "ffmpeg"]
FFMPEG_BINARY = None
for p in POSSIBLE_FFMPEG_PATHS:
    if not p:
        continue
    try:
        subprocess.run([p, "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
        FFMPEG_BINARY = p
        break
    except Exception:
        continue
if FFMPEG_BINARY is None:
    logging.warning("ffmpeg binary not found. Set FFMPEG_BINARY env var or place ffmpeg in ./ffmpeg or /usr/bin/ffmpeg")

POSSIBLE_FFPROBE_PATHS = []
for p in POSSIBLE_FFMPEG_PATHS:
    if p and p.endswith("ffmpeg"):
        POSSIBLE_FFPROBE_PATHS.append(p.replace("ffmpeg", "ffprobe"))
POSSIBLE_FFPROBE_PATHS += ["ffprobe", "/usr/bin/ffprobe", "/usr/local/bin/ffprobe", "./ffprobe"]
FFPROBE_BINARY = None
for p in POSSIBLE_FFPROBE_PATHS:
    if not p:
        continue
    try:
        subprocess.run([p, "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
        FFPROBE_BINARY = p
        break
    except Exception:
        continue

SOURCE_LANGS = ['English', 'Arabic', 'Spanish']
DUB_LANGS = ["Somali"]

LANG_CODE_ASR = {
    'English': 'en',
    'Arabic': 'ar',
    'Spanish': 'es',
}

TTS_VOICE_SINGLE = "it-IT-MarcelloMultilingualNeural"

user_data = {}
processing_active = False
processing_user = None
pending_queue = []

bot = telebot.TeleBot(TELEGRAM_TOKEN)

app = Flask(__name__)

def send_gemini_translation(text, source_lang, target_lang):
    prompt_text = f"Translate the following text from {source_lang} into {target_lang} Write numbers as they are pronounced in Somali (for example, “laba kun iyo labaatan iyo shan” instead of 2025). Do not add any introduction or explanation: {text}"
    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
    headers = {
        "Content-Type": "application/json",
        "X-goog-api-key": GEMINI_KEY
    }
    data = {"contents":[{"parts":[{"text": prompt_text}]}]}
    try:
        response = requests.post(url, headers=headers, json=data, timeout=60)
    except Exception:
        return None
    if response.status_code != 200:
        return None
    try:
        body = response.json()
        translated = None
        if 'candidates' in body and isinstance(body['candidates'], list) and len(body['candidates']) > 0:
            c = body['candidates'][0]
            if 'content' in c and 'parts' in c['content'] and len(c['content']['parts']) > 0:
                translated = c['content']['parts'][0].get('text')
        if not translated and 'output' in body:
            translated = body['output']
        if not translated:
            translated = json.dumps(body)[:3000]
        if isinstance(translated, str):
            for prefix in ["Here is your translation:", "Translation:", "Translated text:", "Output:"]:
                if translated.strip().startswith(prefix):
                    translated = translated.strip()[len(prefix):].strip()
            translated = translated.strip()
        return translated
    except Exception:
        return None

def check_video_size_duration(file_path):
    max_size = 20 * 1024 * 1024
    size = os.path.getsize(file_path)
    if size > max_size:
        return False, "⚠️Cabbirka Video ga ayaa ka badan xadka 20 MB."
    return True, ""

async def generate_tts(text, output_path, voice=TTS_VOICE_SINGLE):
    communicate = edge_tts.Communicate(text, voice)
    await communicate.save(output_path)

def ffprobe_duration(file_path):
    probe = FFPROBE_BINARY if FFPROBE_BINARY else "ffprobe"
    try:
        proc = subprocess.run([probe, "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        out = proc.stdout.decode().strip()
        if not out:
            return 0.0
        return float(out)
    except Exception:
        return 0.0

def build_atempo_chain(x):
    factors = []
    val = x
    while val > 2.0:
        factors.append(2.0)
        val /= 2.0
    while val < 0.5:
        factors.append(0.5)
        val /= 0.5
    factors.append(val)
    return factors

def adjust_audio_to_match_video(video_path, audio_path, output_path):
    audio_dur = ffprobe_duration(audio_path)
    video_dur = ffprobe_duration(video_path)
    if audio_dur <= 0 or video_dur <= 0:
        return audio_path
    audio_speed = audio_dur / video_dur
    if abs(audio_speed - 1.0) < 0.03:
        return audio_path
    if audio_speed <= 0.05 or audio_speed > 10.0:
        return audio_path
    factors = build_atempo_chain(audio_speed)
    filter_str = ",".join([f"atempo={f:.6f}" for f in factors])
    ffmpeg_bin = FFMPEG_BINARY if FFMPEG_BINARY else "ffmpeg"
    try:
        proc = subprocess.run([ffmpeg_bin, "-y", "-i", audio_path, "-filter:a", filter_str, output_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
        if proc.returncode == 0 and os.path.exists(output_path):
            return output_path
        else:
            return audio_path
    except Exception:
        return audio_path

def merge_audio_video(video_path, audio_path, output_path):
    ffmpeg_bin = FFMPEG_BINARY if FFMPEG_BINARY else "ffmpeg"
    cmd = f'{ffmpeg_bin} -y -i "{video_path}" -i "{audio_path}" -map 0:v:0 -map 1:a:0 -c:v copy -c:a aac -shortest -movflags +faststart "{output_path}"'
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.returncode != 0:
        logging.error("FFmpeg error: %s", process.stderr.decode())
    return process.returncode == 0

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "👋 Salaam, soo dhawoow. Fadlan ii soo dir video-(ilaa 20MB ah) si aan kuugu turjumo. Adeeggu waa hal video hal mar.")

@bot.message_handler(commands=['help'])
def help_command(message):
    help_text = ("help? Contact: @boyso20")
    bot.send_message(message.chat.id, help_text)

@bot.message_handler(content_types=['video'])
def handle_video(message):
    global pending_queue
    try:
        if getattr(message.video, "file_size", None) and message.video.file_size > 20 * 1024 * 1024:
            bot.send_message(message.chat.id, "Cabbirka fiidiyowgu wuxuu ka sarreeyaa xadka 20 MB. Fadlan soo dir muuqaal yar.")
            return
        file_info = bot.get_file(message.video.file_id)
        if file_info.file_path is None:
            bot.send_message(message.chat.id, "Nasiib darro ma awoodo inaan ka shaqeeyo video-gaas. Fadlan isku day file kale.")
            return
        downloaded_file = bot.download_file(file_info.file_path)
        timestamp = int(time.time())
        file_path = f'temp_{message.from_user.id}_{timestamp}.mp4'
        with open(file_path, 'wb') as f:
            f.write(downloaded_file)
        valid, msg = check_video_size_duration(file_path)
        if not valid:
            bot.send_message(message.chat.id, f"Warning: {msg}")
            os.remove(file_path)
            return
        user_id = message.from_user.id
        user_entry = user_data.get(user_id, {})
        user_entry['video_path'] = file_path
        user_entry['dub_lang'] = "Somali"
        user_data[user_id] = user_entry
        global processing_active
        if processing_active:
            pending_queue.append({'user_id': user_id, 'video_path': file_path, 'chat_id': message.chat.id})
            return
        markup = types.InlineKeyboardMarkup(row_width=2)
        buttons = [types.InlineKeyboardButton(text=lang, callback_data=f"src|{lang}") for lang in SOURCE_LANGS]
        markup.add(*buttons)
        bot.send_message(message.chat.id, "Select the original language spoken in the video:", reply_markup=markup)
    except Exception:
        logging.exception("Error handling video")
        bot.send_message(message.chat.id, "An error occurred while processing your video. Please try again.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('src|'))
def callback_query(call):
    global processing_active, processing_user
    user_id = call.from_user.id
    data = call.data or ""
    if not data.startswith("src|"):
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        return
    lang = data.split("|", 1)[1]
    if user_id not in user_data:
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        return
    user_data[user_id]['source_lang'] = lang
    user_data[user_id]['dub_lang'] = "Somali"
    try:
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Processing...")
        status = {'chat_id': call.message.chat.id, 'message_id': call.message.message_id}
        user_data[user_id]['status_msg'] = status
    except Exception:
        pass
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    if processing_active:
        try:
            bot.send_message(call.message.chat.id, "Botku hadda wuu mashquulsan yahay. Video-gaaga wuu keydinayaa oo marka la dhammeeyo wuu ku wargelin doonaa.")
        except Exception:
            pass
        return
    processing_active = True
    processing_user = user_id
    try:
        asyncio.run(process_video(call.message.chat.id, user_data[user_id]))
    except Exception:
        logging.exception("Error running process_video")
    finally:
        processing_active = False
        processing_user = None
        if pending_queue:
            next_item = pending_queue.pop(0)
            nid = next_item['user_id']
            npath = next_item['video_path']
            nchat = next_item.get('chat_id')
            user_data[nid] = {'video_path': npath, 'dub_lang': "Somali"}
            markup = types.InlineKeyboardMarkup(row_width=2)
            buttons = [types.InlineKeyboardButton(text=lang, callback_data=f"src|{lang}") for lang in SOURCE_LANGS]
            markup.add(*buttons)
            try:
                bot.send_message(nchat, "Hadda waa fursaddaada. Select the original language spoken in the video:", reply_markup=markup)
            except Exception:
                pass

async def process_video(chat_id, data):
    video_path = data['video_path']
    source_lang = data.get('source_lang', 'English')
    dub_lang = data.get('dub_lang', 'Somali')
    status_info = data.get('status_msg')
    try:
        if status_info:
            try:
                bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Uploading for transcription...")
            except Exception:
                pass
        headers = {'authorization': ASSEMBLYAI_KEY, 'content-type': 'application/octet-stream'}
        upload_url = None
        try:
            with open(video_path, 'rb') as f:
                response = requests.post('https://api.assemblyai.com/v2/upload', headers=headers, data=f, timeout=180)
            if response.status_code in (200, 201):
                upload_url = response.json().get('upload_url')
            else:
                upload_url = None
        except Exception:
            upload_url = None
        if not upload_url:
            if status_info:
                try:
                    bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Failed to upload.")
                except:
                    pass
            else:
                bot.send_message(chat_id, "Failed to upload video for transcription. Please try again.")
            return
        if status_info:
            try:
                bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Submitted for transcription...")
            except Exception:
                pass
        trans_text = ""
        transcript_attempts = 0
        max_transcript_attempts = 3
        transcript_success = False
        while transcript_attempts < max_transcript_attempts and not transcript_success:
            trans_req_payload = {'audio_url': upload_url, 'language_code': LANG_CODE_ASR.get(source_lang, 'en'), 'speaker_labels': True}
            try:
                trans_resp = requests.post(
                    'https://api.assemblyai.com/v2/transcript',
                    headers={'authorization': ASSEMBLYAI_KEY, 'content-type': 'application/json'},
                    json=trans_req_payload,
                    timeout=30
                )
            except Exception:
                trans_resp = None
            if not trans_resp or trans_resp.status_code not in (200, 201):
                transcript_attempts += 1
                time.sleep(2)
                continue
            trans_id = trans_resp.json().get('id')
            retry_count = 0
            max_retries = 60
            while retry_count < max_retries:
                time.sleep(3)
                try:
                    status_resp = requests.get(f'https://api.assemblyai.com/v2/transcript/{trans_id}',
                                             headers={'authorization': ASSEMBLYAI_KEY}, timeout=30)
                except Exception:
                    status_resp = None
                if not status_resp or status_resp.status_code != 200:
                    if status_info:
                        try:
                            bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Error checking transcription status.")
                        except:
                            pass
                    else:
                        bot.send_message(chat_id, "Error checking transcription status. Please try again.")
                    return
                status = status_resp.json()
                if status.get('status') == 'completed':
                    utterances = status.get('utterances') or []
                    speakers = set()
                    for u in utterances:
                        sp = u.get('speaker')
                        if sp:
                            speakers.add(sp)
                    if len(speakers) > 1:
                        user_msg = "Waan ka xumahay isoo dir video hal qof hadlaayo si aan u turjumo ma awoodi karo turjumaad video-yada multiple speakers ah"
                        try:
                            bot.send_message(chat_id, user_msg)
                        except Exception:
                            pass
                        try:
                            if os.path.exists(video_path):
                                os.remove(video_path)
                        except Exception:
                            pass
                        try:
                            if isinstance(chat_id, int) and chat_id in user_data:
                                del user_data[chat_id]
                        except Exception:
                            pass
                        return
                    trans_text = status.get('text', '')
                    transcript_success = True
                    break
                elif status.get('status') == 'failed':
                    transcript_attempts += 1
                    break
                retry_count += 1
            if not transcript_success:
                time.sleep(2)
        if not transcript_success:
            if status_info:
                try:
                    bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Transcription failed.")
                except:
                    pass
            else:
                bot.send_message(chat_id, "Transcription failed after multiple attempts. Please try again.")
            return
        if not trans_text:
            if status_info:
                try:
                    bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: No transcript returned.")
                except:
                    pass
            else:
                bot.send_message(chat_id, "No transcript text returned.")
            return
        if status_info:
            try:
                bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Translating...")
            except Exception:
                pass
        translated_text = None
        trans_attempts = 0
        max_trans_attempts = 3
        while trans_attempts < max_trans_attempts and not translated_text:
            translated_text = send_gemini_translation(trans_text, source_lang, dub_lang)
            if translated_text:
                break
            trans_attempts += 1
            time.sleep(2)
        if not translated_text:
            if status_info:
                try:
                    bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Translation failed.")
                except:
                    pass
            else:
                bot.send_message(chat_id, "Translation failed after multiple attempts. Please try again.")
            return
        translated_text = translated_text.replace(".", ",")
        if status_info:
            try:
                bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Generating speech...")
            except Exception:
                pass
        tts_path = f'tts_{chat_id}.mp3'
        voice = TTS_VOICE_SINGLE
        await generate_tts(translated_text, tts_path, voice)
        if status_info:
            try:
                bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Aligning durations...")
            except Exception:
                pass
        adjusted_tts = f'tts_{chat_id}_adj.mp3'
        final_audio = adjust_audio_to_match_video(video_path, tts_path, adjusted_tts)
        if status_info:
            try:
                bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Merging audio...")
            except Exception:
                pass
        output_path = f'dubbed_{chat_id}.mp4'
        success = merge_audio_video(video_path, final_audio, output_path)
        if not success:
            if status_info:
                try:
                    bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Merge failed.")
                except:
                    pass
            else:
                bot.send_message(chat_id, "Failed to merge audio and video.")
            return
        if status_info:
            try:
                bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: Done.")
            except Exception:
                pass
        else:
            bot.send_message(chat_id, "Your dubbed video is ready. Sending now...")
        try:
            with open(output_path, 'rb') as video_file:
                bot.send_video(chat_id, video_file, supports_streaming=True)
        except Exception:
            bot.send_message(chat_id, "Failed to send video. Please try again.")
        try:
            if os.path.exists(output_path):
                os.remove(output_path)
        except Exception:
            pass
    except Exception:
        logging.exception("Error processing video")
        try:
            if status_info:
                bot.edit_message_text(chat_id=status_info['chat_id'], message_id=status_info['message_id'], text="Processing: An error occurred.")
            else:
                bot.send_message(chat_id, "An error occurred while processing your video. Please try again.")
        except Exception:
            pass
    finally:
        tts_path = f'tts_{chat_id}.mp3'
        adj_path = f'tts_{chat_id}_adj.mp3'
        try:
            if os.path.exists(video_path):
                os.remove(video_path)
        except Exception:
            pass
        try:
            if os.path.exists(tts_path):
                os.remove(tts_path)
        except Exception:
            pass
        try:
            if os.path.exists(adj_path):
                os.remove(adj_path)
        except Exception:
            pass
        try:
            if isinstance(chat_id, int) and chat_id in user_data:
                del user_data[chat_id]
        except Exception:
            pass

@bot.message_handler(content_types=['text'])
def handle_text_inputs(message):
    user_id = message.from_user.id
    if message.text.startswith('/'):
        return
    if user_id not in user_data:
        user_data[user_id] = {}
    bot.send_message(message.chat.id, "Send a video to dub into Somali or use /start to see instructions.")

@app.route("/", methods=["GET", "POST", "HEAD"])
def webhook():
    if request.method in ("GET", "HEAD"):
        return "OK", 200
    if request.method == "POST":
        content_type = request.headers.get("Content-Type", "")
        if content_type and content_type.startswith("application/json"):
            update = telebot.types.Update.de_json(request.get_data().decode("utf-8"))
            bot.process_new_updates([update])
            return "", 200
    return abort(403)

@app.route("/set_webhook", methods=["GET", "POST"])
def set_webhook_route():
    try:
        bot.set_webhook(url=WEBHOOK_URL)
        return f"Webhook set to {WEBHOOK_URL}", 200
    except Exception as e:
        logging.error(f"Failed to set webhook: {e}")
        return f"Failed to set webhook: {e}", 500

@app.route("/delete_webhook", methods=["GET", "POST"])
def delete_webhook_route():
    try:
        bot.delete_webhook()
        return "Webhook deleted.", 200
    except Exception as e:
        logging.error(f"Failed to delete webhook: {e}")
        return f"Failed to delete webhook: {e}", 500

def set_webhook_on_startup():
    try:
        bot.delete_webhook()
        time.sleep(1)
        bot.set_webhook(url=WEBHOOK_URL)
        logging.info(f"Main bot webhook set successfully to {WEBHOOK_URL}")
    except Exception as e:
        logging.error(f"Failed to set main bot webhook on startup: {e}")

def set_bot_info_and_startup():
    set_webhook_on_startup()

if __name__ == "__main__":
    set_bot_info_and_startup()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
