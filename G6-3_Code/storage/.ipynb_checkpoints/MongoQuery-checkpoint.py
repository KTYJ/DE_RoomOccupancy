from pymongo import MongoClient
from tabulate import tabulate

class LibraryMongoAnalyzer:
    """
    Class to analyze library occupancy, lighting, and ideal study spots from MongoDB.
    """

    def __init__(self, mongo_uri, db_name="library"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

    def busiest_hour(self):
        """Fetch the busiest hour within a day"""
        hourly_doc = self.db.agg_hourly_trends.find_one(sort=[("avg_occupancy", -1)])
        print("📈 Busiest Hour Within a Day:")
        if hourly_doc:
            print(f"Hour: {hourly_doc['hour_of_day']}")
            print(f"Average Occupancy: {hourly_doc['avg_occupancy']}")
            print(f"Category: {hourly_doc.get('category', 'N/A')}")
        else:
            print("No hourly data found.")

    def busiest_day(self):
        """Fetch the busiest day of the week"""
        daily_doc = self.db.agg_daily_trends.find_one(sort=[("avg_occupancy", -1)])
        print("\n📆 Busiest Day of the Week:")
        if daily_doc:
            print(f"Day: {daily_doc['day_of_week']}")
            print(f"Average Occupancy: {daily_doc['avg_occupancy']}")
            print(f"Category: {daily_doc.get('category', 'N/A')}")
        else:
            print("No daily data found.")

    def space_utilization(self):
        """Display space utilization for all rooms and top used areas"""
        print("\n🏛️ Space Utilization Analysis (Rooms 1-5):")
        all_rooms = list(self.db.space_optimization.find().sort("room", 1))

        top_usage = sorted(all_rooms, key=lambda x: x["average_usage_percent"], reverse=True)[:1]

        print("\nMost Used Areas :")
        for doc in top_usage:
            print(f"Room: {doc['room']}")
            print(f"Area: {doc['area']}")
            print(f"Capacity: {doc['capacity']}")
            print(f"Avg Occ: {doc['avg_occ']}")
            print(f"Max Occ: {doc['max_occ']}")
            print(f"Usage%: {doc['average_usage_percent']}")
            print(f"Category: {doc.get('category','N/A')}")

        print("\nUnder Utilized Room Estimation:")
        for doc in all_rooms:
            print(f"Room: {doc['room']}, Area: {doc['area']}, Avg Occ: {doc['avg_occ']}, Usage%: {doc['average_usage_percent']}, Category: {doc.get('category','N/A')}")


    def close_connection(self):
        """Close MongoDB connection"""
        self.client.close()


# ----------------------------
# Main execution
# ----------------------------
if __name__ == "__main__":
    MONGO_URI = "mongodb+srv://klwongwm21_db_user:1RtBdiUtFojf2GMf@wongkahlok.rbyed6t.mongodb.net/?appName=WongKahLok"
    analyzer = LibraryMongoAnalyzer(MONGO_URI)

    analyzer.busiest_hour()
    analyzer.busiest_day()
    analyzer.space_utilization()

    analyzer.close_connection()