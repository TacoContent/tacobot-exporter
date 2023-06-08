from pymongo import MongoClient
from bson.objectid import ObjectId
import traceback
import json
import typing
import datetime
import pytz
import os
import uuid

# from .mongodb import migration


class MongoDatabase:
    def __init__(self):
        self.client = None
        self.connection = None
        pass

    def open(self):
        if "MONGODB_URL" not in os.environ or os.environ["MONGODB_URL"] == "":
            raise ValueError("MONGODB_URL is not set")
        self.client = MongoClient(os.environ["MONGODB_URL"])
        self.connection = self.client.tacobot

    def close(self):
        try:
            if self.client:
                self.client.close()
        except Exception as ex:
            print(ex)
            traceback.print_exc()

    def get_sum_all_tacos(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(
                self.connection.tacos.aggregate(
                    [{"$match": {"guild_id": str(guild_id)}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
                )
            )
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_sum_all_gift_tacos(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(
                self.connection.taco_gifts.aggregate(
                    [{"$match": {"guild_id": str(guild_id)}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
                )
            )
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_sum_all_taco_reactions(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.tacos_reactions.count({"guild_id": str(guild_id)})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_sum_all_twitch_tacos(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(
                self.connection.twitch_tacos_gifts.aggregate(
                    [
                        {
                            "$match": {"guild_id": str(guild_id)},
                        },
                        {
                            "$group": {"_id": None, "total": {"$sum": "$count"}}
                        },
                    ]
                )
            )
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_live_now_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.live_tracked.count({"guild_id": str(guild_id)})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_twitch_channel_bot_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.twitch_channels.count({"guild_id": str(guild_id)})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_twitch_linked_accounts_count(self):
        try:
            if self.connection is None:
                self.open()
            return self.connection.twitch_user.count()
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_tqotd_questions_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.tqotd.count({"guild_id": str(guild_id)})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_tqotd_answers_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.tqotd.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": None,
                        "total": { "$sum": { "$size": "$answered"}}
                    }
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_invited_users_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.invite_codes.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": None,
                        "total": { "$sum": { "$size": { "$ifNull": [ "$invites", []] } } }
                    }
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_sum_live_twitch(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.live_activity.count({
                "$and": [
                    {
                        "status": { "$eq": "ONLINE"},
                        "guild_id": str(guild_id)
                    },
                    { "platform": "TWITCH" }
                ]
                })
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_sum_live_youtube(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.live_activity.count({
                "$and": [
                    {
                        "status": { "$eq": "ONLINE"},
                        "guild_id": str(guild_id)
                    },
                    { "platform": "YOUTUBE" }
                ]
                })
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_wdyctw_questions_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.wdyctw.count({"guild_id": str(guild_id)})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_wdyctw_answers_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.wdyctw.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": None,
                        "total": { "$sum": { "$size": "$answered"}}
                    }
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_techthurs_questions_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.techthurs.count({"guild_id": str(guild_id)})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_techthurs_answers_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.techthurs.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": None,
                        "total": { "$sum": { "$size": "$answered"}}
                    }
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_mentalmondays_questions_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.mentalmondays.count({"guild_id": str(guild_id)})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_mentalmondays_answers_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.mentalmondays.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": None,
                        "total": { "$sum": { "$size": "$answered"}}
                    }
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_tacotuesday_questions_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.taco_tuesday.count({"guild_id": str(guild_id)})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_tacotuesday_answers_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.taco_tuesday.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": None,
                        "total": { "$sum": { "$size": "$answered"}}
                    }
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_game_keys_available_count(self):
        try:
            if self.connection is None:
                self.open()
            return self.connection.game_keys.count({ "redeemed_by": { "$eq": None }})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_game_keys_redeemed_count(self):
        try:
            if self.connection is None:
                self.open()
            return self.connection.game_keys.count({ "redeemed_by": { "$ne": None }})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_minecraft_whitelisted_count(self):
        try:
            if self.connection is None:
                self.open()
            return self.connection.minecraft_users.count({ "whitelisted": { "$eq": True }})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_logs_count(self, guild_id: int, level: str = None):
        try:
            if self.connection is None:
                self.open()
            if level:
                return self.connection.logs.count({ "guild_id": int(guild_id), "level": { "$eq": level } })
            else:
                return self.connection.logs.count({ "guild_id": int(guild_id) })
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_team_requests_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.stream_team_requests.count({ "guild_id": int(guild_id) })
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_birthdays_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.birthdays.count({ "guild_id": int(guild_id) })
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_first_messages_today_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.first_message.count({ "guild_id": str(guild_id), "date": { "$gte": datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time()) } })
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_messages_tracked_count(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return self.connection.messages.count({ "guild_id": str(guild_id) })
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_known_users(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.users.find({ "guild_id": str(guild_id) }))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()
