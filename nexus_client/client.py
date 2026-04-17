import asyncio
import json
import logging
from typing import Any, Callable

import aiohttp
import paho.mqtt.client as mqtt
import paho.mqtt.publish as mqttpublish

logger = logging.getLogger(__name__)


class NexusClient:
    """Client MQTT authentifié via Voight-Kampff.

    Le mot de passe MQTT est soit une API key VK, soit un cookie de session VK.
    Voight-Kampff valide les deux formes via /mqtt/user.

    Usage avec cookie de session (navigateur) :
        client = await NexusClient.from_session_cookie(vk_url, mqtt_host, cookie)
        await client.publish("common/foo", {"hello": "world"})

    Usage avec API key (service) :
        client = NexusClient.from_api_key(vk_url, mqtt_host, username, api_key)
        client.subscribe("common/#", my_callback)
        async with client:
            await asyncio.sleep(...)
    """

    def __init__(self, vk_url: str, mqtt_host: str, mqtt_port: int = 1883):
        self._vk_url = vk_url.rstrip("/")
        self._mqtt_host = mqtt_host
        self._mqtt_port = mqtt_port
        self._username: str | None = None
        self._password: str | None = None
        self._paho: mqtt.Client | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscriptions: dict[str, list[Callable]] = {}

    # ── Factories ─────────────────────────────────────────────────────────────

    @classmethod
    async def from_session_cookie(
        cls,
        vk_url: str,
        mqtt_host: str,
        session_cookie: str,
        mqtt_port: int = 1883,
    ) -> "NexusClient":
        """Résout le username via VK et prépare le client avec le cookie comme mot de passe."""
        instance = cls(vk_url, mqtt_host, mqtt_port)
        instance._password = session_cookie
        instance._username = await instance._resolve_username(session_cookie)
        return instance

    @classmethod
    def from_api_key(
        cls,
        vk_url: str,
        mqtt_host: str,
        username: str,
        api_key: str,
        mqtt_port: int = 1883,
    ) -> "NexusClient":
        """Crée le client avec une API key VK comme mot de passe MQTT."""
        instance = cls(vk_url, mqtt_host, mqtt_port)
        instance._username = username
        instance._password = api_key
        return instance

    # ── Propriétés ────────────────────────────────────────────────────────────

    @property
    def username(self) -> str | None:
        return self._username

    @property
    def password(self) -> str | None:
        return self._password

    # ── Auth VK ───────────────────────────────────────────────────────────────

    async def _resolve_username(self, session_cookie: str) -> str:
        try:
            async with aiohttp.ClientSession() as http:
                resp = await http.get(
                    f"{self._vk_url}/verify",
                    headers={
                        "Cookie": f"vk_session={session_cookie}",
                        "Accept": "application/json",
                    },
                )
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("user", "anonymous")
        except Exception as e:
            logger.warning(f"Résolution username VK échouée: {e}")
        return "anonymous"

    # ── Publish ───────────────────────────────────────────────────────────────

    async def publish(self, topic: str, payload: Any, retain: bool = False) -> None:
        """Publie un message sur un topic MQTT (connexion one-shot).

        retain=True : Mosquitto conserve le dernier message — tout nouvel abonné
        le reçoit immédiatement, utile pour les manifestes de service.
        """
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: mqttpublish.single(
                topic=topic,
                payload=payload,
                hostname=self._mqtt_host,
                port=self._mqtt_port,
                auth={"username": self._username, "password": self._password},
                retain=retain,
            ),
        )
        logger.debug(f"MQTT publié sur {topic} (retain={retain})")

    # ── Subscribe ─────────────────────────────────────────────────────────────

    def subscribe(self, topic: str, callback: Callable) -> None:
        """Enregistre un callback pour un topic (wildcards # et + supportés).

        Le callback reçoit (topic: str, payload: Any).
        Les fonctions async et sync sont toutes deux acceptées.
        """
        self._subscriptions.setdefault(topic, []).append(callback)
        if self._paho and self._paho.is_connected():
            self._paho.subscribe(topic)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"MQTT connecté: {self._username}")
            for topic in self._subscriptions:
                client.subscribe(topic)
        else:
            logger.error(f"Connexion MQTT refusée: code {rc}")

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode())
        except Exception:
            payload = msg.payload.decode()

        for pattern, callbacks in self._subscriptions.items():
            if mqtt.topic_matches_sub(pattern, topic):
                for cb in callbacks:
                    if asyncio.iscoroutinefunction(cb) and self._loop:
                        asyncio.run_coroutine_threadsafe(cb(topic, payload), self._loop)
                    else:
                        cb(topic, payload)

    def start_listening(self) -> None:
        """Démarre la boucle MQTT dans un thread séparé (non bloquant)."""
        self._loop = asyncio.get_event_loop()
        self._paho = mqtt.Client()
        self._paho.username_pw_set(self._username, self._password)
        self._paho.on_connect = self._on_connect
        self._paho.on_message = self._on_message
        self._paho.connect(self._mqtt_host, self._mqtt_port)
        self._paho.loop_start()
        logger.info(f"MQTT écoute démarrée: {self._username}@{self._mqtt_host}:{self._mqtt_port}")

    def stop_listening(self) -> None:
        """Arrête la boucle MQTT."""
        if self._paho:
            self._paho.loop_stop()
            self._paho.disconnect()
            self._paho = None
            logger.info("MQTT écoute arrêtée")

    # ── Context manager ───────────────────────────────────────────────────────

    async def __aenter__(self) -> "NexusClient":
        self.start_listening()
        return self

    async def __aexit__(self, *_) -> None:
        self.stop_listening()
