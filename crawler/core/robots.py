from collections import defaultdict
class RobotsParser:
    @staticmethod
    def parse(content: str) -> list:
        if not content:
            return []
        user_agents = defaultdict(lambda: {"allow": [], "disallow": [], "crawl_delay": None})
        lines = content.splitlines()
        current_agents = []
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            key, value = map(str.strip, line.split(":", 1))
            key_lower = key.lower()
            if key_lower == "user-agent":
                ua = value.strip()
                current_agents = [ua] if ua else []
                continue
            if key_lower == "disallow" and current_agents:
                for ua in current_agents:
                    user_agents[ua]["disallow"].append(value)
        disallowed = []
        for ua_data in user_agents.values():
            disallowed.extend(ua_data["disallow"])
        disallowed = [d for d in disallowed if d]
        return sorted(set(disallowed))