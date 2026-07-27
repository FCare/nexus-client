#!/usr/bin/env python3
"""
Test de NexusClient avec Authentik OAuth.

Usage:
    1. Créer l'application dans Authentik via le script shell
    2. Exporter les credentials :
       export AUTHENTIK_CLIENT_ID="nexus-test-client"
       export AUTHENTIK_CLIENT_SECRET="le-secret-généré"
    3. Lancer ce script : python test_authentik.py
"""

import asyncio
import os
import sys

# Ajouter le chemin pour importer nexus_client
sys.path.insert(0, os.path.dirname(__file__))

import aiohttp
from nexus_client import NexusClient


AUTHENTIK_URL = os.getenv("AUTHENTIK_URL", "https://sso.caronboulme.fr")
CLIENT_ID = os.getenv("AUTHENTIK_CLIENT_ID")
CLIENT_SECRET = os.getenv("AUTHENTIK_CLIENT_SECRET")
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")


async def get_client_credentials_token():
    """Obtient un access token via Client Credentials grant."""
    print(f"🔐 Demande d'access token à {AUTHENTIK_URL}...")

    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            f"{AUTHENTIK_URL}/application/o/token/",
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )

        if resp.status != 200:
            text = await resp.text()
            print(f"❌ Erreur HTTP {resp.status}: {text}")
            return None

        data = await resp.json()
        access_token = data.get("access_token")

        if access_token:
            print(f"✅ Token obtenu : {access_token[:20]}...{access_token[-10:]}")
            print(f"   Expire dans : {data.get('expires_in')} secondes")
            print(f"   Token type : {data.get('token_type')}")
            return access_token
        else:
            print(f"❌ Pas de token dans la réponse: {data}")
            return None


async def test_nexus_client():
    """Test de la résolution de username via NexusClient."""
    print("\n" + "="*60)
    print("TEST : NexusClient.from_authentik_token()")
    print("="*60 + "\n")

    # Étape 1 : Obtenir un token
    access_token = await get_client_credentials_token()
    if not access_token:
        print("\n❌ Impossible d'obtenir un token. Test abandonné.")
        return False

    # Étape 2 : Créer le NexusClient
    print(f"\n🔧 Création du NexusClient...")
    try:
        client = await NexusClient.from_authentik_token(
            authentik_url=AUTHENTIK_URL,
            mqtt_host=MQTT_HOST,
            access_token=access_token,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        print(f"✅ NexusClient créé avec succès !")
        print(f"   Username résolu : {client.username}")
        print(f"   MQTT host : {MQTT_HOST}")

        if client.username == "anonymous":
            print("\n⚠️  Username = 'anonymous' : le token est peut-être invalide")
            print("    ou Authentik n'a pas retourné de username dans l'introspection.")
            return False
        else:
            print(f"\n✅ TEST RÉUSSI : Username '{client.username}' résolu via Authentik !")
            return True

    except Exception as e:
        print(f"❌ Erreur lors de la création du NexusClient : {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Point d'entrée principal."""
    print("\n" + "🧪 " * 30)
    print("TEST NEXUS-CLIENT AVEC AUTHENTIK OAUTH")
    print("🧪 " * 30 + "\n")

    # Vérifier les variables d'environnement
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Variables manquantes !")
        print("\nVous devez définir :")
        print("  export AUTHENTIK_CLIENT_ID='nexus-test-client'")
        print("  export AUTHENTIK_CLIENT_SECRET='le-secret-généré'")
        print("\nPour obtenir ces credentials, lancez :")
        print("  bash /tmp/create_nexus_test_app.sh")
        return 1

    print(f"Configuration :")
    print(f"  AUTHENTIK_URL     : {AUTHENTIK_URL}")
    print(f"  CLIENT_ID         : {CLIENT_ID}")
    print(f"  CLIENT_SECRET     : {CLIENT_SECRET[:10]}...{CLIENT_SECRET[-5:]}")
    print(f"  MQTT_HOST         : {MQTT_HOST}")
    print()

    success = await test_nexus_client()

    print("\n" + "="*60)
    if success:
        print("✅ TOUS LES TESTS RÉUSSIS !")
    else:
        print("❌ TESTS ÉCHOUÉS")
    print("="*60 + "\n")

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
