import discord
from discord.ext import commands
import json
import os
import asyncio
import aiohttp
import time
from datetime import datetime, timezone

# ========================== AYARLAR ==========================
BOT_TOKEN = os.environ.get('BOT_TOKEN', 'BOT_TOKEN_BURAYA')
DATA_FILE = 'user_data.json'

# ========================== VERİ TABANI ==========================
def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_data(data):
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

userData = load_data()
afkClients = {}

# ========================== VOICE CLIENT (WEBSOCKET) ==========================
class VoiceClient:
    def __init__(self, token, channel_id, user_id):
        self.token = token
        self.channel_id = channel_id
        self.user_id = user_id
        self.ws = None
        self.voice_ws = None
        self.session = None
        self.heartbeat_task = None
        self.voice_heartbeat_task = None
        self.presence_task = None
        self.connected = False
        self.voice_connected = False
        self.running = True
        
        self.guild_id = None
        self.username = "Unknown"
        self.session_id = None
        self.voice_token = None
        self.voice_endpoint = None
        self.rpc_start_time = None
    
    async def get_user_info(self):
        """Kullanıcı bilgilerini al"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://discord.com/api/v9/users/@me",
                    headers={"Authorization": self.token}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.username = data['username']
                        self.user_id = data['id']
                        return True
                    return False
        except Exception as e:
            print(f'✗ User info hatası: {e}')
            return False
    
    async def get_channel_info(self):
        """Kanal bilgilerini al"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://discord.com/api/v9/channels/{self.channel_id}",
                    headers={"Authorization": self.token}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.guild_id = data.get('guild_id')
                        return True
                    return False
        except Exception as e:
            print(f'✗ Channel info hatası: {e}')
            return False
    
    def get_rpc_payload(self):
        """RPC payload hazırla"""
        if self.rpc_start_time is None:
            self.rpc_start_time = int(time.time() * 1000)
        
        return {
            "status": "dnd",
            "since": 0,
            "activities": [{
                "name": "ZULÜMSÜZ AFK Sistemi",
                "type": 0,
                "state": "7/24 AFK Aktif",
                "details": "zulümsüz",
                "timestamps": {
                    "start": self.rpc_start_time
                },
                "application_id": "1466710246482510026",
                "assets": {
                    "large_image": "zulumler",
                    "large_text": "ZULÜMSÜZ AFK Sistemi"
                },
                "buttons": [
                    "ZULÜMSÜZ Sunucusuna Katıl",
                    "© 2026 Zulümsüz"
                ],
                "metadata": {
                    "button_urls": [
                        "https://discord.gg/zulumsuz",
                        "https://discord.gg/zulumsuz"
                    ]
                }
            }],
            "afk": False
        }
    
    async def update_presence(self):
        """RPC durumunu güncelle"""
        try:
            if self.ws and not self.ws.closed:
                await self.ws.send_json({
                    "op": 3,
                    "d": self.get_rpc_payload()
                })
                print(f'✓ RPC güncellendi: {self.username}')
                return True
        except Exception as e:
            print(f'! RPC hata: {e}')
        return False
    
    async def presence_updater(self):
        """Her 3 dakikada bir RPC güncelle"""
        try:
            while self.running and self.ws and not self.ws.closed:
                await asyncio.sleep(180)
                await self.update_presence()
        except:
            pass
    
    async def connect_gateway(self):
        """Gateway'e bağlan"""
        try:
            self.session = aiohttp.ClientSession()
            
            ws_url = "wss://gateway.discord.gg/?v=10&encoding=json"
            self.ws = await self.session.ws_connect(ws_url)
            
            hello = await self.ws.receive_json()
            interval = hello['d']['heartbeat_interval']
            
            self.heartbeat_task = asyncio.create_task(self.heartbeat(interval))
            
            await self.ws.send_json({
                "op": 2,
                "d": {
                    "token": self.token,
                    "properties": {
                        "os": "Windows 10",
                        "browser": "Discord Client",
                        "device": "desktop"
                    },
                    "presence": self.get_rpc_payload(),
                    "compress": False,
                    "intents": 513
                }
            })
            
            print(f'→ Gateway bağlanıyor: {self.username}')
            
            start_time = time.time()
            async for msg in self.ws:
                if time.time() - start_time > 10:
                    print(f'✗ Gateway timeout: {self.username}')
                    return False
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    
                    if data.get('op') == 10:
                        continue
                    
                    if data.get('t') == 'READY':
                        self.session_id = data['d']['session_id']
                        print(f'✓ Gateway READY: {self.username}')
                        
                        asyncio.create_task(self.first_presence_update())
                        self.presence_task = asyncio.create_task(self.presence_updater())
                        
                        return True
            
            return False
            
        except Exception as e:
            print(f'✗ Gateway hata: {e}')
            return False
    
    async def first_presence_update(self):
        """İlk RPC güncellemesi"""
        await asyncio.sleep(2)
        await self.update_presence()
        await asyncio.sleep(3)
        await self.update_presence()
    
    async def connect_voice(self):
        """Voice kanalına bağlan"""
        try:
            # Voice State Update gönder
            await self.ws.send_json({
                "op": 4,
                "d": {
                    "guild_id": self.guild_id,
                    "channel_id": self.channel_id,
                    "self_mute": False,
                    "self_deaf": True,
                    "self_video": False
                }
            })
            
            print(f'→ Sese bağlanıyor: {self.username}')
            
            # Voice events bekle
            voice_state = None
            voice_server = None
            
            start_time = time.time()
            async for msg in self.ws:
                if time.time() - start_time > 8:
                    print(f'✗ Voice timeout: {self.username}')
                    break
                    
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    
                    if data.get('t') == 'VOICE_STATE_UPDATE':
                        voice_state = data
                    if data.get('t') == 'VOICE_SERVER_UPDATE':
                        voice_server = data
                        self.voice_token = data['d']['token']
                        self.voice_endpoint = data['d']['endpoint'].replace(':80', '')
                    
                    if voice_state and voice_server:
                        break
            
            if not voice_server:
                print(f'✗ Voice server bilgisi yok: {self.username}')
                return False
            
            # Voice WebSocket
            voice_ws_url = f"wss://{self.voice_endpoint}/?v=4"
            self.voice_ws = await self.session.ws_connect(voice_ws_url)
            
            # Voice Hello
            voice_hello = await self.voice_ws.receive_json()
            voice_interval = voice_hello['d']['heartbeat_interval']
            
            # Voice Heartbeat başlat
            self.voice_heartbeat_task = asyncio.create_task(
                self.voice_heartbeat(voice_interval)
            )
            
            # Voice Identify
            await self.voice_ws.send_json({
                "op": 0,
                "d": {
                    "server_id": self.guild_id,
                    "user_id": self.user_id,
                    "session_id": self.session_id,
                    "token": self.voice_token
                }
            })
            
            # Voice Ready bekle
            start_time = time.time()
            async for msg in self.voice_ws:
                if time.time() - start_time > 5:
                    print(f'✗ Voice ready timeout: {self.username}')
                    return False
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data.get('op') == 2:  # Ready
                        self.voice_connected = True
                        self.connected = True
                        print(f'✓✓✓ SESE BAĞLANDI: {self.username} ✓✓✓')
                        
                        # RPC'yi tekrar güncelle
                        asyncio.create_task(self.update_after_voice())
                        
                        # Event loop'ları başlat
                        asyncio.create_task(self.gateway_event_loop())
                        asyncio.create_task(self.voice_event_loop())
                        
                        return True
            
            return False
            
        except Exception as e:
            print(f'✗ Voice bağlantı hatası: {e}')
            return False
    
    async def update_after_voice(self):
        """Sese bağlandıktan sonra RPC güncelle"""
        await asyncio.sleep(1)
        await self.update_presence()
    
    async def heartbeat(self, interval):
        """Gateway heartbeat"""
        try:
            interval_sec = interval / 1000
            while self.ws and not self.ws.closed and self.running:
                await self.ws.send_json({"op": 1, "d": None})
                await asyncio.sleep(interval_sec)
        except:
            pass
    
    async def voice_heartbeat(self, interval):
        """Voice heartbeat"""
        try:
            interval_sec = interval / 1000
            while self.voice_ws and not self.voice_ws.closed and self.running:
                await self.voice_ws.send_json({"op": 3, "d": int(time.time() * 1000)})
                await asyncio.sleep(interval_sec)
        except:
            pass
    
    async def gateway_event_loop(self):
        """Gateway event loop"""
        try:
            async for msg in self.ws:
                if not self.running:
                    break
                if msg.type == aiohttp.WSMsgType.CLOSED:
                    print(f'! Gateway kapandı: {self.username}')
                    self.connected = False
                    self.voice_connected = False
                    break
        except:
            self.connected = False
            self.voice_connected = False
    
    async def voice_event_loop(self):
        """Voice event loop"""
        try:
            async for msg in self.voice_ws:
                if not self.running:
                    break
                if msg.type == aiohttp.WSMsgType.CLOSED:
                    print(f'! Voice kapandı: {self.username}')
                    self.voice_connected = False
                    break
        except:
            self.voice_connected = False
    
    async def start(self):
        """Başlat"""
        print(f'[BAŞLATILIYOR] {self.username if self.username != "Unknown" else "Hesap"}')
        
        if not await self.get_user_info():
            print(f'✗ User info alınamadı')
            return False
        
        if not await self.get_channel_info():
            print(f'✗ Channel info alınamadı: {self.username}')
            return False
        
        if not await self.connect_gateway():
            print(f'✗ Gateway bağlantısı başarısız: {self.username}')
            return False
        
        # Gateway bağlandıktan hemen sonra voice'a geç
        await asyncio.sleep(0.5)
        
        if not await self.connect_voice():
            print(f'✗ Voice bağlantısı başarısız: {self.username}')
            return False
        
        return True
    
    async def stop(self):
        """Durdur"""
        print(f'[DURDURULUYOR] {self.username}')
        self.running = False
        self.connected = False
        self.voice_connected = False
        
        # Taskları iptal et
        tasks = [self.heartbeat_task, self.voice_heartbeat_task, self.presence_task]
        for task in tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # WebSocket'leri kapat
        if self.voice_ws and not self.voice_ws.closed:
            try:
                await self.voice_ws.close()
            except:
                pass
        
        if self.ws and not self.ws.closed:
            try:
                await self.ws.close()
            except:
                pass
        
        # Session'ı kapat
        if self.session and not self.session.closed:
            try:
                await self.session.close()
            except:
                pass
        
        print(f'✓ Bağlantı kesildi: {self.username}')

# ========================== AFK FONKSİYONLARI ==========================
async def startAFK(userId, token, channelId):
    """AFK başlat"""
    key = f"{userId}_{channelId}"
    
    if key in afkClients:
        print(f'! Zaten çalışıyor: {key}')
        return False
    
    try:
        client = VoiceClient(token, channelId, None)
        
        if await client.start():
            afkClients[key] = {
                'client': client,
                'channelId': channelId,
                'startTime': time.time()
            }
            print(f'✓ AFK başlatıldı: {key}')
            return True
        else:
            await client.stop()
            return False
            
    except Exception as e:
        print(f'✗ AFK başlatma hatası: {e}')
        return False

async def stopAFK(userId, channelId):
    """AFK durdur"""
    key = f"{userId}_{channelId}"
    
    if key not in afkClients:
        print(f'! Bulunamadı: {key}')
        return False
    
    try:
        client = afkClients[key]['client']
        await client.stop()
        del afkClients[key]
        print(f'✓ AFK durduruldu: {key}')
        return True
    except Exception as e:
        print(f'✗ AFK durdurma hatası: {e}')
        return False

# ========================== BOT ==========================
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix='.', intents=intents)

@bot.event
async def on_ready():
    print(f'✓ Bot hazır: {bot.user}')
    print(f'✓ Sunucu sayısı: {len(bot.guilds)}')
    
    # Bot restart olsa bile eski tokenları yeniden başlat
    if userData:
        print('→ Kayıtlı tokenlar yeniden başlatılıyor...')
        for userId in userData:
            for token_data in userData[userId].get('tokens', []):
                try:
                    await asyncio.sleep(2)
                    username = token_data.get('username', 'Unknown')
                    if await startAFK(userId, token_data['token'], token_data['channelId']):
                        print(f'✓ Yeniden başlatıldı: {username}')
                    else:
                        print(f'✗ Yeniden başlatılamadı: {username}')
                except Exception as e:
                    print(f'✗ Yeniden başlatma hatası: {e}')

# ========================== PANEL KOMUT ==========================
@bot.command(name='panel')
async def panel(ctx):
    embed = discord.Embed(
        title="ZULÜMSÜZ VC TOKEN BOTU",
        description="Token tabanlı sesli kanal bağlantı sistemi",
        color=0xFFFFFF
    )
    
    embed.add_field(
        name="<a:soru:1467146889316143156> Ne İşe Yarar?",
        value="Bu bot, kayıtlı token'lar ile otomatik olarak sesli kanallara bağlanmanızı sağlar. "
              "Birden fazla hesabı aynı anda yönetebilir ve istediğiniz sesli kanala bağlayabilirsiniz.",
        inline=False
    )
    
    embed.add_field(
        name="<a:ayarcik:1467146425874776202> Nasıl Çalışır?",
        value="1. **Token Ekle** butonuna tıklayarak token ekleyin\n"
              "2. Kanal ID ve token bilgilerini girin\n"
              "3. Bot otomatik olarak Discord Gateway üzerinden bağlanacak\n"
              "4. WebSocket bağlantısı ile sesli kanala katılım sağlanacak\n"
              "5. `.tokencontrol` komutu ile aktif bağlantıları görüntüleyin",
        inline=False
    )
    
    embed.add_field(
        name="<a:guvenlik:1467147068811382817> Güvenlik",
        value="• Tüm veriler güvenli şekilde saklanır\n"
              "• WebSocket bağlantısı ile güvenli iletişim\n"
              "• Sadece siz kendi tokenlerinizi yönetebilirsiniz\n"
              "• Bot restart olsa bile tokenlar korunur ve otomatik yeniden bağlanır",
        inline=False
    )
    
    embed.set_image(url="https://i.ibb.co/Hf79drb/image.png")
    embed.set_footer(text="ZULÜMSÜZ • Güvenli Token Sistemi")
    embed.timestamp = datetime.now(timezone.utc)
    
    view = PanelButtons(str(ctx.author.id))
    await ctx.send(embed=embed, view=view)

# ========================== PANEL BUTTONS ==========================
class PanelButtons(discord.ui.View):
    def __init__(self, userId):
        super().__init__(timeout=None)
        self.userId = userId
    
    @discord.ui.button(label='Token Ekle', style=discord.ButtonStyle.green, custom_id='btn_token_add_single')
    async def token_add(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bu senin panelin değil!', ephemeral=True)
            return
        
        modal = TokenEkleModal(self.userId)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label='Toplu Token Ekle', style=discord.ButtonStyle.blurple, custom_id='btn_token_add_bulk')
    async def token_bulk(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bu senin panelin değil!', ephemeral=True)
            return
        
        modal = TopluTokenModal(self.userId)
        await interaction.response.send_modal(modal)

# ========================== MODAL: TEK TOKEN ==========================
class TokenEkleModal(discord.ui.Modal, title='Token Ekle'):
    def __init__(self, userId):
        super().__init__()
        self.userId = userId
        
        self.kanal = discord.ui.TextInput(
            label='Kanal ID',
            placeholder='Sesli kanal ID giriniz',
            required=True,
            style=discord.TextStyle.short
        )
        
        self.token = discord.ui.TextInput(
            label='User Token',
            placeholder='Discord user token giriniz',
            required=True,
            style=discord.TextStyle.paragraph
        )
        
        self.add_item(self.kanal)
        self.add_item(self.token)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        channelId = self.kanal.value.strip()
        token = self.token.value.strip()
        
        # Token'dan kullanıcı adını al
        username = "Unknown"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://discord.com/api/v9/users/@me",
                    headers={"Authorization": token}
                ) as resp:
                    if resp.status == 200:
                        user_data = await resp.json()
                        username = user_data['username']
                    else:
                        await interaction.followup.send('❌ Geçersiz token!', ephemeral=True)
                        return
        except Exception as e:
            await interaction.followup.send(f'❌ Token doğrulanamadı: {e}', ephemeral=True)
            return
        
        # Kullanıcı verisini hazırla
        if self.userId not in userData:
            userData[self.userId] = {'tokens': []}
        
        # Token'ı kaydet
        userData[self.userId]['tokens'].append({
            'token': token,
            'channelId': channelId,
            'username': username,
            'addedAt': datetime.now(timezone.utc).isoformat()
        })
        save_data(userData)
        
        await interaction.followup.send(f'⏳ **{username}** ekleniyor...', ephemeral=True)
        
        # 2 saniye bekle
        await asyncio.sleep(2)
        
        # Bağlan
        if await startAFK(self.userId, token, channelId):
            await interaction.followup.send(f'✅ **{username}** başarıyla bağlandı!', ephemeral=True)
        else:
            await interaction.followup.send(f'❌ **{username}** eklendi ama bağlantı başarısız!', ephemeral=True)

# ========================== MODAL: TOPLU TOKEN ==========================
class TopluTokenModal(discord.ui.Modal, title='Toplu Token Ekle'):
    def __init__(self, userId):
        super().__init__()
        self.userId = userId
        
        self.kanal = discord.ui.TextInput(
            label='Kanal ID',
            placeholder='Sesli kanal ID giriniz',
            required=True,
            style=discord.TextStyle.short
        )
        
        self.tokens = discord.ui.TextInput(
            label='Token Listesi',
            placeholder='Her satıra bir token yazınız',
            required=True,
            style=discord.TextStyle.paragraph,
            max_length=4000
        )
        
        self.add_item(self.kanal)
        self.add_item(self.tokens)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        channelId = self.kanal.value.strip()
        token_list = [t.strip() for t in self.tokens.value.split('\n') if t.strip()]
        
        # Kullanıcı verisini hazırla
        if self.userId not in userData:
            userData[self.userId] = {'tokens': []}
        
        embed = discord.Embed(
            title="<a:Durumloading:1467149969604481218> İşleniyor...",
            description=f"**{len(token_list)}** token işleniyor...",
            color=0xFFFFFF
        )
        msg = await interaction.followup.send(embed=embed, ephemeral=True)
        
        success = 0
        failed = 0
        
        for idx, token in enumerate(token_list, 1):
            try:
                # Token'dan kullanıcı adını al
                username = "Unknown"
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        "https://discord.com/api/v9/users/@me",
                        headers={"Authorization": token}
                    ) as resp:
                        if resp.status == 200:
                            user_data = await resp.json()
                            username = user_data['username']
                        else:
                            failed += 1
                            continue
                
                # Kaydet
                userData[self.userId]['tokens'].append({
                    'token': token,
                    'channelId': channelId,
                    'username': username,
                    'addedAt': datetime.now(timezone.utc).isoformat()
                })
                save_data(userData)
                
                # İlerleme göster
                embed.description = f"**{idx}/{len(token_list)}** işleniyor...\n**{username}** ekleniyor..."
                await msg.edit(embed=embed)
                
                # 2 saniye bekle
                await asyncio.sleep(2)
                
                # Bağlan
                if await startAFK(self.userId, token, channelId):
                    success += 1
                else:
                    failed += 1
                    
            except Exception as e:
                failed += 1
                print(f'Toplu ekleme hatası: {e}')
        
        embed = discord.Embed(
            title="İşlem Tamamlandı",
            description=f"<:acik:1467150438422810726> **Başarılı:** {success}\n"
                       f"<:kapali:1467150331300417671> **Başarısız:** {failed}",
            color=0x00FF00 if success > 0 else 0xFF0000
        )
        await msg.edit(embed=embed)

# ========================== TOKEN CONTROL ==========================
@bot.command(name='tokencontrol')
async def tokencontrol(ctx):
    userId = str(ctx.author.id)
    
    if userId not in userData or not userData[userId].get('tokens'):
        embed = discord.Embed(
            description="❌ Kayıtlı token bulunamadı!",
            color=0xFF0000
        )
        await ctx.send(embed=embed)
        return
    
    tokens = userData[userId]['tokens']
    aktif = sum(1 for t in tokens if f"{userId}_{t['channelId']}" in afkClients 
                and afkClients[f"{userId}_{t['channelId']}"]['client'].voice_connected)
    
    embed = discord.Embed(
        title="<:kullanici:1467149592129699963> Token Kontrol Paneli",
        description=f"**Toplam:** {len(tokens)} hesap\n"
                   f"<:kulaklik_acik:1467149139166101667> **Aktif:** {aktif}\n"
                   f"<:kulaklik2_kapali:1467149140441038957> **Pasif:** {len(tokens) - aktif}",
        color=0xFFFFFF
    )
    
    view = TokenSelectMenu(userId)
    await ctx.send(embed=embed, view=view)

# ========================== TOKEN SELECT MENU ==========================
class TokenSelectMenu(discord.ui.View):
    def __init__(self, userId):
        super().__init__(timeout=180)
        self.userId = userId
        
        tokens = userData[userId]['tokens']
        options = []
        
        for i, token_data in enumerate(tokens[:25]):
            key = f"{userId}_{token_data['channelId']}"
            is_connected = (key in afkClients and 
                          afkClients[key]['client'].voice_connected)
            
            username = token_data.get('username', f'Token #{i+1}')
            status = "Bağlı" if is_connected else "Bağlı değil"
            
            options.append(
                discord.SelectOption(
                    label=username[:100],
                    value=str(i),
                    description=status
                )
            )
        
        select = discord.ui.Select(
            placeholder="Bir token seçin",
            options=options,
            custom_id="token_select_menu_main"
        )
        select.callback = self.select_callback
        self.add_item(select)
        
        # Tümünü Seç butonu
        btn = discord.ui.Button(
            label='Tümünü Seç',
            style=discord.ButtonStyle.success,
            custom_id='select_all_btn_main'
        )
        btn.callback = self.select_all_callback
        self.add_item(btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bu senin panelin değil!', ephemeral=True)
            return
        
        index = int(interaction.data['values'][0])
        token_data = userData[self.userId]['tokens'][index]
        
        key = f"{self.userId}_{token_data['channelId']}"
        is_connected = (key in afkClients and 
                       afkClients[key]['client'].voice_connected)
        
        username = token_data.get('username', f'Token #{index+1}')
        
        embed = discord.Embed(
            title=f"<:kullanici:1467149592129699963> {username}",
            color=0xFFFFFF
        )
        
        if is_connected:
            embed.add_field(name='Durum', value='<:acik:1467150438422810726> Bağlı', inline=True)
            embed.add_field(name='Bağlantı', value='<:kulaklik_acik:1467149139166101667> Aktif', inline=True)
        else:
            embed.add_field(name='Durum', value='<:kapali:1467150331300417671> Bağlı Değil', inline=True)
            embed.add_field(name='Bağlantı', value='<:kulaklik2_kapali:1467149140441038957> Pasif', inline=True)
        
        embed.add_field(name='Kanal ID', value=token_data['channelId'], inline=False)
        
        view = TokenControlButtons(self.userId, index)
        await interaction.response.edit_message(embed=embed, view=view)
    
    async def select_all_callback(self, interaction: discord.Interaction):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bu senin panelin değil!', ephemeral=True)
            return
        
        tokens = userData[self.userId]['tokens']
        aktif = sum(1 for t in tokens if f"{self.userId}_{t['channelId']}" in afkClients 
                   and afkClients[f"{self.userId}_{t['channelId']}"]['client'].voice_connected)
        
        embed = discord.Embed(
            title='<a:premium:1467150620958920900> Toplu Kontrol Paneli',
            color=0xFFD700
        )
        embed.add_field(name='Toplam Hesap', value=len(tokens), inline=True)
        embed.add_field(name='<:acik:1467150438422810726> Aktif', value=aktif, inline=True)
        embed.add_field(name='<:kapali:1467150331300417671> Pasif', value=len(tokens) - aktif, inline=True)
        
        view = AllControlButtons(self.userId)
        await interaction.response.edit_message(embed=embed, view=view)

# ========================== SINGLE TOKEN CONTROL ==========================
class TokenControlButtons(discord.ui.View):
    def __init__(self, userId, index):
        super().__init__(timeout=180)
        self.userId = userId
        self.index = index
    
    @discord.ui.button(label='Durdur', style=discord.ButtonStyle.danger, emoji='<:cop:1467155465699069994>', custom_id='btn_stop_single')
    async def durdur(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bu senin tokenin değil!', ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        token_data = userData[self.userId]['tokens'][self.index]
        
        if await stopAFK(self.userId, token_data['channelId']):
            await interaction.followup.send('✅ Durduruldu!', ephemeral=True)
        else:
            await interaction.followup.send('❌ Durdurma başarısız!', ephemeral=True)
    
    @discord.ui.button(label='Yeniden Başlat', style=discord.ButtonStyle.primary, emoji='<a:yenilenme:1467155246693351508>', custom_id='btn_restart_single')
    async def yenile(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bu senin tokenin değil!', ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        token_data = userData[self.userId]['tokens'][self.index]
        
        await stopAFK(self.userId, token_data['channelId'])
        await asyncio.sleep(2)
        
        if await startAFK(self.userId, token_data['token'], token_data['channelId']):
            await interaction.followup.send('✅ Yeniden başlatıldı!', ephemeral=True)
        else:
            await interaction.followup.send('❌ Başlatma başarısız!', ephemeral=True)
    
    @discord.ui.button(label='Kanal Değiştir', style=discord.ButtonStyle.success, row=1, custom_id='btn_change_channel_single')
    async def kanal_degistir(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bu senin tokenin değil!', ephemeral=True)
            return
        
        modal = KanalDegistirModal(self.userId, self.index)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label='Sil', style=discord.ButtonStyle.secondary, row=1, emoji='<:cop:1467155465699069994>', custom_id='btn_delete_single')
    async def sil(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bu senin tokenin değil!', ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        token_data = userData[self.userId]['tokens'][self.index]
        
        await stopAFK(self.userId, token_data['channelId'])
        
        userData[self.userId]['tokens'].pop(self.index)
        save_data(userData)
        
        key = f"{self.userId}_{token_data['channelId']}"
        if key in afkClients:
            try:
                del afkClients[key]
            except:
                pass
        
        await interaction.followup.send('✅ Token silindi!', ephemeral=True)

# ========================== ALL CONTROL BUTTONS ==========================
class AllControlButtons(discord.ui.View):
    def __init__(self, userId):
        super().__init__(timeout=180)
        self.userId = userId
    
    @discord.ui.button(label='Hepsini Durdur', style=discord.ButtonStyle.danger, emoji='<:cop:1467155465699069994>', custom_id='btn_stop_all')
    async def durdur_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bunlar senin tokenin değil!', ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        stopped = 0
        for token in userData[self.userId]['tokens']:
            if await stopAFK(self.userId, token['channelId']):
                stopped += 1
            await asyncio.sleep(1)
        
        await interaction.followup.send(f'✅ {stopped} hesap durduruldu!', ephemeral=True)
    
    @discord.ui.button(label='Hepsini Yenile', style=discord.ButtonStyle.primary, emoji='<a:yenilenme:1467155246693351508>', custom_id='btn_restart_all')
    async def yenile_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bunlar senin tokenin değil!', ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        restarted = 0
        for token in userData[self.userId]['tokens']:
            await stopAFK(self.userId, token['channelId'])
            await asyncio.sleep(1)
            if await startAFK(self.userId, token['token'], token['channelId']):
                restarted += 1
            await asyncio.sleep(3)
        
        await interaction.followup.send(f'✅ {restarted} hesap yeniden başlatıldı!', ephemeral=True)
    
    @discord.ui.button(label='Tümünün Kanalını Değiştir', style=discord.ButtonStyle.success, row=1, custom_id='btn_change_all')
    async def kanal_degistir_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bunlar senin tokenin değil!', ephemeral=True)
            return
        
        modal = TopluKanalDegistirModal(self.userId)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label='Tümünü Sil', style=discord.ButtonStyle.secondary, row=1, emoji='<:cop:1467155465699069994>', custom_id='btn_delete_all')
    async def sil_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.userId:
            await interaction.response.send_message('❌ Bunlar senin tokenin değil!', ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        deleted = 0
        for token in userData[self.userId]['tokens'][:]:
            try:
                await stopAFK(self.userId, token['channelId'])
                deleted += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f'Silme hatası: {e}')
        
        userData[self.userId]['tokens'] = []
        save_data(userData)
        
        keys_to_remove = [key for key in afkClients.keys() if key.startswith(f"{self.userId}_")]
        for key in keys_to_remove:
            try:
                del afkClients[key]
            except:
                pass
        
        await interaction.followup.send(f'✅ {deleted} hesap silindi!', ephemeral=True)

# ========================== KANAL DEĞİŞTİR MODALS ==========================
class KanalDegistirModal(discord.ui.Modal, title='Kanal Değiştir'):
    def __init__(self, userId, index):
        super().__init__()
        self.userId = userId
        self.index = index
        
        self.kanal = discord.ui.TextInput(
            label='Yeni Kanal ID',
            placeholder='Yeni sesli kanal ID giriniz',
            required=True,
            style=discord.TextStyle.short
        )
        self.add_item(self.kanal)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        new_channel = self.kanal.value.strip()
        token_data = userData[self.userId]['tokens'][self.index]
        old_channel = token_data['channelId']
        
        # Durdur
        await stopAFK(self.userId, old_channel)
        
        # Güncelle
        userData[self.userId]['tokens'][self.index]['channelId'] = new_channel
        save_data(userData)
        
        # Yeniden başlat
        await asyncio.sleep(2)
        if await startAFK(self.userId, token_data['token'], new_channel):
            await interaction.followup.send('✅ Kanal değiştirildi ve bağlandı!', ephemeral=True)
        else:
            await interaction.followup.send('❌ Kanal değiştirildi ama bağlantı başarısız!', ephemeral=True)

class TopluKanalDegistirModal(discord.ui.Modal, title='Toplu Kanal Değiştir'):
    def __init__(self, userId):
        super().__init__()
        self.userId = userId
        
        self.kanal = discord.ui.TextInput(
            label='Yeni Kanal ID',
            placeholder='Tüm tokenlar için yeni kanal ID',
            required=True,
            style=discord.TextStyle.short
        )
        self.add_item(self.kanal)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        new_channel = self.kanal.value.strip()
        
        changed = 0
        for i, token in enumerate(userData[self.userId]['tokens']):
            old_channel = token['channelId']
            
            # Durdur
            await stopAFK(self.userId, old_channel)
            
            # Güncelle
            userData[self.userId]['tokens'][i]['channelId'] = new_channel
            
            # Yeniden başlat
            await asyncio.sleep(2)
            if await startAFK(self.userId, token['token'], new_channel):
                changed += 1
            await asyncio.sleep(1)
        
        save_data(userData)
        await interaction.followup.send(f'✅ {changed} hesabın kanalı değiştirildi!', ephemeral=True)

# ========================== BOT BAŞLAT ==========================
if __name__ == '__main__':
    print('━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    print('🚀 ZULÜMSÜZ VC TOKEN BOT')
    print('━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    bot.run(BOT_TOKEN)
