db = db.getSiblingDB("admin");
db.auth("admin", "admin");
sh.enableSharding("stream_airbnb");
sh.shardCollection("stream_airbnb.reviews", { "listing_id_id_reviewer_id": "hashed" });
sh.enableSharding("stream_zoo");
sh.shardCollection("stream_zoo.tortoise", { "readable_time": "hashed" });
sh.shardCollection("stream_zoo.zoo_analytics", { "_id": "hashed" });

