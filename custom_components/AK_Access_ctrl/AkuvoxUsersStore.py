class AkuvoxUsersStore(Store):
    """
    Persistent registry of HA-managed users, keyed by HA-xxx.

    users: {
      "HA-003": {
         "name": "Jane",
         "groups": ["Default", "Staff"],
         "relays": "3",            # "0".."3"
         "pin": "1234",
         "card_code": "987654",
         "face_url": "/local/faces/jane.jpg",  # URL under /local (optional)
         "phone": "+44....",       # not synced to keypads
         "status": "active|disabled|pending|deleted"  # UI hint (optional)
      },
      ...
    }
    """
    def __init__(self, hass: HomeAssistant):
        super().__init__(hass, 1, USERS_STORAGE_KEY)
        self.data: Dict[str, Any] = {"users": {}}

    async def async_load(self):
        existing = await super().async_load()
        if existing and isinstance(existing.get("users"), dict):
            self.data = existing

    async def async_save(self):
        await super().async_save(self.data)

    def get(self, key: str, default=None):
        return (self.data.get("users") or {}).get(key, default)

    def all(self) -> Dict[str, Any]:
        return dict(self.data.get("users") or {})

    def all_ha_ids(self) -> List[str]:
        return [k for k in (self.data.get("users") or {}).keys() if k.startswith("HA-")]

    def reserve_id(self, ha_id: str):
        self.data["users"].setdefault(ha_id, {})

    async def upsert_profile(
        self,
        key: str,
        *,
        name: Optional[str] = None,
        groups: Optional[List[str]] = None,
        relays: Optional[str] = None,
        pin: Optional[str] = None,
        card_code: Optional[str] = None,
        face_url: Optional[str] = None,
        phone: Optional[str] = None,
        status: Optional[str] = None,
    ):
        u = self.data["users"].setdefault(key, {})
        if name is not None:
            u["name"] = name
        if groups is not None:
            u["groups"] = list(groups)
        if relays is not None:
            u["relays"] = str(relays)
        if pin is not None:
            u["pin"] = str(pin)
        if card_code is not None:
            u["card_code"] = str(card_code)
        if face_url is not None:
            u["face_url"] = face_url
        if phone is not None:
            u["phone"] = str(phone)
        if status is not None:
            u["status"] = status
        await self.async_save()

    async def delete(self, key: str):
        self.data.get("users", {}).pop(key, None)
        await self.async_save()
