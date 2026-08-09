import asyncio
import json
import logging
import os
import uuid
from typing import Any, Callable

import aiohttp
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class NexusClient:
    """Client MQTT authentifié via Authentik OAuth.

    Le mot de passe MQTT est un access token OAuth Authentik.

    Usage avec Authentik OAuth token (navigateur ou service) :
        client = await NexusClient.from_authentik_token(
            authentik_url, mqtt_host, access_token, client_id, client_secret
        )
        await client.publish("common/foo", {"hello": "world"})
    """

    def __init__(self, auth_url: str, mqtt_host: str, mqtt_port: int = 1883):
        self._auth_url = auth_url.rstrip("/")
        self._mqtt_host = mqtt_host
        self._mqtt_port = mqtt_port
        self._username: str | None = None
        self._password: str | None = None
        self._paho: mqtt.Client | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscriptions: dict[str, list[Callable]] = {}

    # ── Factories ─────────────────────────────────────────────────────────────

    @classmethod
    async def from_authentik_token(
        cls,
        authentik_url: str,
        mqtt_host: str,
        access_token: str,
        client_id: str,
        client_secret: str,
        mqtt_port: int = 1883,
    ) -> "NexusClient":
        """Résout le username via Authentik OAuth introspection et prépare le client
        avec le token comme mot de passe MQTT.

        Args:
            authentik_url: URL de base Authentik (ex: https://sso.caronboulme.fr)
            mqtt_host: Hostname du broker MQTT
            access_token: Access token OAuth obtenu après authentification
            client_id: Client ID OAuth de l'application (pour l'introspection)
            client_secret: Client secret OAuth de l'application
            mqtt_port: Port MQTT (défaut 1883)
        """
        instance = cls(authentik_url, mqtt_host, mqtt_port)
        instance._password = access_token
        instance._username = await instance._resolve_username_authentik(
            access_token, client_id, client_secret
        )
        return instance

    # ── Propriétés ────────────────────────────────────────────────────────────

    @property
    def username(self) -> str | None:
        return self._username

    @property
    def password(self) -> str | None:
        return self._password

    # ── Auth Authentik ────────────────────────────────────────────────────────

    async def _resolve_username_authentik(
        self, access_token: str, client_id: str, client_secret: str
    ) -> str:
        """Résout le username via OAuth introspection Authentik.

        Appelle l'endpoint /application/o/introspect/ pour vérifier le token
        et récupérer le username associé.

        Pour les tokens Client Credentials (machine-to-machine), utilise
        le client_id comme username par défaut.
        """
        try:
            async with aiohttp.ClientSession() as http:
                resp = await http.post(
                    f"{self._auth_url}/application/o/introspect/",
                    data={
                        "token": access_token,
                        "client_id": client_id,
                        "client_secret": client_secret,
                    },
                )
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("active"):
                        # Tokens utilisateur ont un username
                        username = data.get("username")
                        if username:
                            logger.info(f"Résolution username Authentik OK: {username}")
                            return username
                        # Tokens Client Credentials : utiliser client_id
                        token_client_id = data.get("client_id")
                        if token_client_id:
                            logger.info(f"Token Client Credentials Authentik OK: {token_client_id}")
                            return token_client_id
                    logger.warning(f"Token Authentik inactif ou invalide")
                else:
                    logger.warning(f"Introspection Authentik échec HTTP {resp.status}")
        except Exception as e:
            logger.warning(f"Résolution username Authentik échouée: {e}")
        return "anonymous"

    # ── Publish ───────────────────────────────────────────────────────────────

    async def publish(self, topic: str, payload: Any, retain: bool = False) -> None:
        """Publie un message sur un topic MQTT.

        Utilise la connexion persistante _paho si disponible (start_listening appelé),
        sinon ouvre une connexion one-shot. Cela évite l'overhead TCP/auth sur chaque
        publish pendant la génération de bulletins ou sous forte charge.

        retain=True : Mosquitto conserve le dernier message — tout nouvel abonné
        le reçoit immédiatement, utile pour les manifestes de service.
        """
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        if self._paho and self._paho.is_connected():
            self._paho.publish(topic, payload, retain=retain)
            logger.debug(f"MQTT publié sur {topic} via connexion persistante (retain={retain})")
        else:
            # Fallback one-shot si pas de connexion persistante
            import paho.mqtt.publish as mqttpublish
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
                    qos=1,
                ),
            )
            logger.debug(f"MQTT publié sur {topic} via connexion one-shot (retain={retain})")

    # ── Subscribe ─────────────────────────────────────────────────────────────

    async def request(self, topic: str, payload: Any, timeout: float = 30.0) -> Any | None:
        """Publie une requête et attend la réponse sur un topic reply/{uuid} éphémère.

        Injecte automatiquement reply_to dans le payload. Retourne le payload de la réponse,
        ou None si timeout.
        """
        cid = str(uuid.uuid4())
        reply_topic = f"reply/{cid}"
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()

        async def _on_reply(_t: str, p: Any) -> None:
            if not future.done():
                future.set_result(p)

        self.subscribe(reply_topic, _on_reply)

        req_payload = {**(payload if isinstance(payload, dict) else {}), "reply_to": reply_topic}
        if not isinstance(payload, dict):
            req_payload["_payload"] = payload

        await self.publish(topic, req_payload)

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self.unsubscribe(reply_topic)

    def unsubscribe(self, topic: str) -> None:
        """Se désabonne d'un topic."""
        self._subscriptions.pop(topic, None)
        if self._paho and self._paho.is_connected():
            self._paho.unsubscribe(topic)

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

        # Snapshot: _on_message tourne sur le thread réseau paho pendant que
        # subscribe() peut muter _subscriptions depuis la boucle asyncio —
        # itérer directement dessus a déjà crashé le thread avec
        # "RuntimeError: dictionary changed size during iteration".
        for pattern, callbacks in list(self._subscriptions.items()):
            if mqtt.topic_matches_sub(pattern, topic):
                for cb in callbacks:
                    if asyncio.iscoroutinefunction(cb) and self._loop:
                        asyncio.run_coroutine_threadsafe(cb(topic, payload), self._loop)
                    else:
                        cb(topic, payload)

    def start_listening(self) -> None:
        """Démarre la boucle MQTT dans un thread séparé (non bloquant).

        Idempotent : si un client paho est déjà connecté (ex: nexus_client partagé entre
        plusieurs sessions d'un même utilisateur, chacune appelant start_listening() à son
        tour), ne pas en recréer un second. Sans cette garde, chaque appel supplémentaire
        ouvrait une connexion MQTT de plus, toutes abonnées aux mêmes topics (subscribe()
        s'appuie sur le même client déjà connecté) — un message publié une seule fois était
        alors livré une fois par connexion vivante, déclenchant le callback en double, triple,
        etc. au fil des sessions, avec des résultats non déterministes à chaque doublon.
        """
        if self._paho and self._paho.is_connected():
            logger.debug(f"MQTT déjà connecté: {self._username}@{self._mqtt_host}:{self._mqtt_port}, skip")
            return
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
