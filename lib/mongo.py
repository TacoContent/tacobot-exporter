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

# single query to get sums of values for exporter
    def get_exporter_sum_data(self, guild_id: int):
        try:
            all_taco_sum = self.get_sum_all_tacos(guild_id)
            all_gift_taco_sum = self.get_sum_all_gift_tacos(guild_id)
            all_reaction_taco_sum = self.get_sum_all_taco_reactions(guild_id)
            all_twitch_taco_sum = self.get_sum_all_twitch_tacos(guild_id)
            live_now_sum = self.get_live_now_count(guild_id)
            twitch_channel_bot = self.get_twitch_channel_bot_count(guild_id)
            twitch_linked_accounts = self.get_twitch_linked_accounts_count()
            tqotd = self.get_tqotd_questions_count(guild_id)
            tqotd_answers = self.get_tqotd_answers_count(guild_id)
            invited_users = self.get_invited_users_count(guild_id)
            live_twitch = self.get_sum_live_twitch(guild_id)
            live_youtube = self.get_sum_live_youtube(guild_id)
            wdyctw = self.get_wdyctw_questions_count(guild_id)
            wdyctw_answers = self.get_wdyctw_answers_count(guild_id)
            techthurs = self.get_techthurs_questions_count(guild_id)
            techthurs_answers = self.get_techthurs_answers_count(guild_id)
            mentalmondays = self.get_mentalmondays_questions_count(guild_id)
            mentalmondays_answers = self.get_mentalmondays_answers_count(guild_id)
            tacotuesday = self.get_tacotuesday_questions_count(guild_id)
            tacotuesday_answers = self.get_tacotuesday_answers_count(guild_id)
            game_keys_available = self.get_game_keys_available_count()
            game_keys_redeemed = self.get_game_keys_redeemed_count()
            minecraft_whitelisted = self.get_minecraft_whitelisted_count()
            team_requests = self.get_team_requests_count(guild_id)
            birthdays = self.get_birthdays_count(guild_id)
            first_messages_today = self.get_first_messages_today_count(guild_id)

            return {
                "all_tacos": all_taco_sum[0]['total'],
                "all_gift_tacos": all_gift_taco_sum[0]['total'],
                "all_reaction_tacos": all_reaction_taco_sum,
                "all_twitch_tacos": all_twitch_taco_sum[0]['total'],
                "live_now": live_now_sum,
                "twitch_channels": twitch_channel_bot,
                "twitch_linked_accounts": twitch_linked_accounts,
                "tqotd": tqotd,
                "tqotd_answers": tqotd_answers[0]['total'],
                "invited_users": invited_users[0]['total'],
                "live_twitch": live_twitch,
                "live_youtube": live_youtube,
                "wdyctw": wdyctw,
                "wdyctw_answers": wdyctw_answers[0]['total'],
                "techthurs": techthurs,
                "techthurs_answers": techthurs_answers[0]['total'],
                "mentalmondays": mentalmondays,
                "mentalmondays_answers": mentalmondays_answers[0]['total'],
                "tacotuesday": tacotuesday,
                "tacotuesday_answers": tacotuesday_answers[0]['total'],
                "game_keys_available": game_keys_available,
                "game_keys_redeemed": game_keys_redeemed,
                "minecraft_whitelisted": minecraft_whitelisted,
                "stream_team_requests": team_requests,
                "birthdays": birthdays,
                "first_messages_today": first_messages_today,
            }
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

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
            return self.connection.minecraft_users.count({ "whitelist": { "$eq": True }})
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_logs(self, guild_id: int, level: str = None):
        try:
            if self.connection is None:
                self.open()
            if level:
                return self.connection.logs.find({ "guild_id": int(guild_id), "level": { "$eq": level } })
            else:
                return self.connection.logs.find({ "guild_id": int(guild_id) })
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
            return self.connection.birthdays.count({ "guild_id": str(guild_id) })
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
            # get UTC time for midnight today
            utc_today = datetime.datetime.combine(datetime.datetime.utcnow().today(), datetime.datetime.min.time())
            # convert utc_today to unix timestamp
            utc_today_ts = int((utc_today - datetime.datetime(1970, 1, 1)).total_seconds())

            return self.connection.first_message.count({ "guild_id": str(guild_id), "timestamp": { "$gte": utc_today_ts } })
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

    def get_user_messages_tracked(self, guild_id: int, limit: int = 10):
        try:
            if self.connection is None:
                self.open()
            # get the top limit messages from users.
            # join the users collection to get the username
            # sort by count descending

            return list(self.connection.messages.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": "$user_id",
                        "count": { "$sum": { "$size": "$messages" } }
                    }
                },
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "user"
                    }
                },
                {
                    # remove items where the user is not found
                    "$match": { "user": { "$ne": [] } }
                },
                # remove items where the user is a bot
                {
                    "$match": { "user.bot": { "$ne": True } }
                },
                {
                    "$sort": { "count": -1 }
                },
                {
                    "$limit": limit
                }
            ]))
            # return list(self.connection.messages.find({ "guild_id": str(guild_id) }).sort("count", -1).limit(limit))
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

    def get_top_taco_gifters(self, guild_id: int, limit: int = 10):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.taco_gifts.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": "$user_id",
                        "count": { "$sum": "$count"}
                    }
                },
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "user"
                    }
                },
                {
                    # remove items where the user is not found
                    "$match": { "user": { "$ne": [] } }
                },
                # remove items where the user is a bot
                {
                    "$match": { "user.bot": { "$ne": True } }
                },
                {
                    "$sort": { "count": -1 }
                },
                {
                    "$limit": limit
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_top_taco_reactors(self, guild_id: int, limit: int = 10):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.tacos_reactions.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": "$user_id",
                        "count": { "$sum": 1 }
                    }
                },
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "user"
                    }
                },
                {
                    # remove items where the user is not found
                    "$match": { "user": { "$ne": [] } }
                },
                # remove items where the user is a bot
                {
                    "$match": { "user.bot": { "$ne": True } }
                },
                {
                    "$sort": { "count": -1 }
                },
                {
                    "$limit": limit
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_top_taco_receivers(self, guild_id: int, limit: int = 10):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.tacos.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": "$user_id",
                        "count": { "$sum": "$count"}
                    }
                },
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "user"
                    }
                },
                {
                    # remove items where the user is not found
                    "$match": { "user": { "$ne": [] } }
                },
                # remove items where the user is a bot
                {
                    "$match": { "user.bot": { "$ne": True } }
                },
                {
                    "$sort": { "count": -1 }
                },
                {
                    "$limit": limit
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_live_activity(self, guild_id: int, limit: int = 10):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.live_activity.aggregate([
                {
                    "$match": {"guild_id": str(guild_id), "status": "ONLINE" },
                },
                {
                    "$group": {
                        "_id": "$user_id" ,
                        "count": { "$sum": 1 },
                        #"platform": { "$firstN": { "n": 2, "input": "$platform" } },
                    }
                },
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "user",
                    }
                },
                {
                    # remove items where the user is not found
                    "$match": { "user": { "$ne": [] } }
                },
                # remove items where the user is a bot
                {
                    "$match": { "user.bot": { "$ne": True } }
                },
                {
                    "$sort": { "count": -1 }
                },
                {
                    "$limit": limit
                }
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_suggestions(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.suggestions.find({ "guild_id": str(guild_id) }))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_user_join_leave(self, guild_id: int):
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.user_join_leave.find({ "guild_id": str(guild_id) }))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()

    def get_food_posts_count(self, guild_id: int) -> list:
        try:
            if self.connection is None:
                self.open()
            return list(self.connection.food_posts.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": "$user_id",
                        "count": { "$sum": 1 }
                    }
                },
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "user"
                    }
                },
                {
                    # remove items where the user is not found
                    "$match": { "user": { "$ne": [] } }
                },
                # remove items where the user is a bot
                {
                    "$match": {
                        "user.bot": { "$ne": True },
                        "user.system": { "$ne": True }
                    }
                },
                {
                    "$sort": { "count": -1 }
                },
            ]))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()
            return []

    def get_taco_logs_counts(self, guild_id: int) -> list:
        try:
            if self.connection is None:
                self.open()

            # get logs counts, aggragted by type.
            return list(self.connection.tacos_log.aggregate([
                {
                    "$match": {"guild_id": str(guild_id)},
                },
                {
                    "$group": {
                        "_id": "$type",
                        "count": { "$sum": "$count" }
                    }
                },
                {
                    "$sort": { "count": -1 }
                },
            ]))

        except Exception as ex:
            print(ex)
            traceback.print_exc()
        finally:
            if self.connection:
                self.close()
            return []
