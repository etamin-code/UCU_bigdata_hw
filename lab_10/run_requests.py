import json
import csv
from datetime import timedelta
from builtins import min as b_min
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import to_date, max, min

SPARK = SparkSession.builder.appName('DataProcessing').getOrCreate()


def read_data(spark, file_name):
    data_dict = csv.DictReader(open(file_name))
    df = spark.createDataFrame(data_dict)
    df = df.withColumn("trending_date", to_date(df.trending_date, 'yy.dd.mm'))
    df = df.withColumn("views", df.views.cast('int'))
    return df


def write_data(result_data, file_name):
    json_object = json.dumps(result_data, indent=4)
    with open(file_name, "w") as outfile:
        outfile.write(json_object)


def get_category_name(category_id, file_name):
    with open(file_name, 'r') as json_file:
        for item in json.load(json_file)["items"]:
            if item["id"] == category_id:
                category_name = item["snippet"]["title"]
                return category_name
    return None


def get_top_10_videos_by_days(df):
    trending_videos_json = {"videos": []}
    for video in df.groupBy("video_id").count().sort(desc('count')).limit(10).collect():
        cur_video = df.filter(df.video_id == video.video_id).sort('trending_date').collect()
        cur_video_days_info = []
        for day in cur_video:
            cur_video_days_info.append({
                "date": day.trending_date.strftime("%Y.%d.%m"),
                "views": int(day.views),
                "likes": int(day.likes),
                "dislikes": int(day.dislikes)
            })

        trending_videos_json["videos"].append({
            "id": video.video_id,
            "title": cur_video[0].title,
            "description": cur_video[0].description,
            "latest_views": int(cur_video[-1].views),
            "latest_likes": int(cur_video[-1].likes),
            "latest_dislikes": int(cur_video[-1].dislikes),
            "trending_days": cur_video_days_info
        })
    return trending_videos_json


def get_top_categories_per_week(df, file_with_category):
    weeks_categories_json = {"weeks": []}

    start = df.agg(min('trending_date').alias('start')).collect()[0].start
    end = df.agg(max('trending_date').alias('end')).collect()[0].end

    start_of_week = start
    end_of_week = start + timedelta(days=6)

    while start_of_week <= end:
        cur_week_df = df.filter((df.trending_date >= start_of_week) & (df.trending_date <= end_of_week)).select(
            'video_id', 'category_id', 'trending_date', 'views'
        )
        if cur_week_df.rdd.isEmpty():
            week = {
                "start_date": start_of_week.strftime("%Y.%d.%m"),
                "end_date": end_of_week.strftime("%Y.%d.%m"),
                "category_id": "",
                "category_name": "",
                "number_of_videos": 0,
                "total_views": 0,
                "video_ids": []
            }
            weeks_categories_json["weeks"].append(week)

            start_of_week = start_of_week + timedelta(days=7)
            end_of_week = end_of_week + timedelta(days=7)
            continue
        first_appearance_df = cur_week_df.groupBy('video_id'). \
            agg(min('trending_date').alias('first_appearance')). \
            withColumnRenamed("video_id", "video_id_1")
        first_appearance_df = cur_week_df.join(first_appearance_df,
                                               (first_appearance_df.video_id_1 == cur_week_df.video_id) &
                                               (first_appearance_df.first_appearance == cur_week_df.trending_date),
                                               how='inner'
                                               ). \
            select("video_id", "views", "first_appearance"). \
            withColumnRenamed("views", "start_week_views")

        last_appearance_df = cur_week_df.groupBy('video_id'). \
            agg(max('trending_date').alias('last_appearance')). \
            withColumnRenamed("video_id", "video_id_1")
        last_appearance_df = cur_week_df.join(last_appearance_df,
                                              (last_appearance_df.video_id_1 == cur_week_df.video_id) &
                                              (last_appearance_df.last_appearance == cur_week_df.trending_date),
                                              how='inner'
                                              ). \
            select("video_id", "category_id", "views", "last_appearance"). \
            withColumnRenamed("views", "end_week_views")

        first_and_last_appearance_df = first_appearance_df.join(last_appearance_df, on='video_id')
        first_and_last_appearance_df = first_and_last_appearance_df.withColumn(
            'new_views', first_and_last_appearance_df.end_week_views - first_and_last_appearance_df.start_week_views)

        grouped_by_category = first_and_last_appearance_df.groupBy('category_id'). \
            sum('new_views').withColumnRenamed("sum(new_views)", "new_views").dropna()
        top1_category_id, top1_category_views = grouped_by_category.sort(desc('new_views')).collect()[0]
        top1_category_name = get_category_name(str(top1_category_id), file_with_category)

        video_ids = [id[0] for id in first_and_last_appearance_df.filter(
            first_and_last_appearance_df.category_id == top1_category_id).select("video_id").collect()]
        weeks_categories_json["weeks"].append({
            "start_date": start_of_week.strftime("%Y.%d.%m"),
            "end_date": end_of_week.strftime("%Y.%d.%m"),
            "category_id": top1_category_id,
            "category_name": top1_category_name,
            "number_of_videos": len(video_ids),
            "total_views": top1_category_views,
            "video_ids": video_ids
        })

        start_of_week = start_of_week + timedelta(days=7)
        end_of_week = end_of_week + timedelta(days=7)
    return weeks_categories_json


def get_top10_tags(df):
    trending_tags_json = {"months": []}

    start = df.agg(min('trending_date').alias('min_trending_date')).collect()[0].min_trending_date
    end = df.agg(max('trending_date').alias('max_trending_date')).collect()[0].max_trending_date

    start_of_month = start
    end_of_month = b_min([start_of_month.replace(
        day=1, month=start_of_month.month % 12 + 1, year=start.year + int(start_of_month.month / 12)
    ) - timedelta(days=1), end])

    while start_of_month <= end:
        cur_month_df = df.filter((df.trending_date >= start_of_month) & (df.trending_date <= end_of_month))
        tags_data = cur_month_df.select('video_id', 'tags').dropDuplicates(['video_id']).collect()
        tags_count = dict()
        for video in tags_data:
            for tag in video.tags.replace('"', '').split("|"):
                if tag in tags_count:
                    tags_count[tag].append(video.video_id)
                else:
                    tags_count[tag] = [video.video_id]

        tags = []
        for trending_tag in sorted(tags_count.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
            tags.append({
                "tag": trending_tag[0],
                "number_of_videos": len(trending_tag[1]),
                "video_ids": trending_tag[1]
            })

        trending_tags_json["months"].append({
            "start_date": start_of_month.strftime("%Y.%d.%m"),
            "end_date": end_of_month.strftime("%Y.%d.%m"),
            "tags": tags
        })
        start_of_month = end_of_month + timedelta(days=1)
        end_of_month = b_min(start_of_month.replace(
            day=1, month=start_of_month.month % 12 + 1, year=start_of_month.year + int(
                start_of_month.month / 12)) - timedelta(days=1), end)

    return trending_tags_json


def get_top20_channels_by_views(df):
    top_channels_json = {"channels": []}

    cur_df = df.orderBy('trending_date', ascending=False).coalesce(1).dropDuplicates(subset=['video_id'])
    grouped = cur_df.groupBy('channel_title'). \
        sum('views'). \
        withColumnRenamed("sum(views)", "views"). \
        sort(desc('views')).limit(20). \
        collect()
    for channel in grouped:
        start_date = df.filter(
            df.channel_title == channel.channel_title).agg(
            min('trending_date').alias('start_date')).collect()[0].start_date
        end_date = df.filter(
            df.channel_title == channel.channel_title).agg(
            max('trending_date').alias('end_date')).collect()[0].end_date

        channel_videos = df.filter(df.channel_title == channel.channel_title).collect()
        videos_views = []
        for video in channel_videos:
            videos_views.append({
                "video_id": video.video_id,
                "views": video.views
            })
        top_channels_json["channels"].append({
            "channel_name": channel.channel_title,
            "start_date": start_date.strftime("%Y.%d.%m"),
            "end_date": end_date.strftime("%Y.%d.%m"),
            "total_views": int(channel.views),
            "videos_views": videos_views
        })
    return top_channels_json


def get_top10_channels_by_days(df):
    channels_json = {"channels": []}

    top_channels = df.groupBy('channel_title').count(). \
        sort(desc('count')).limit(10).withColumnRenamed("count", "total_trending_days")
    for channel in top_channels.collect():
        cur_data = df.filter(df.channel_title == channel.channel_title)

        cur_channel_videos = []
        for video in cur_data.dropDuplicates(subset=['video_id']).collect():
            cur_channel_videos.append({
                "video_id": video.video_id,
                "video_title": video.title,
                "trending_days": cur_data.filter(cur_data.video_id == video.video_id).count()
            })

        channels_json["channels"].append({
            "channel_name": channel.channel_title,
            "total_trending_days": int(channel.total_trending_days),
            "videos_days": cur_channel_videos
        })
    return channels_json


def get_top10_video_by_ratio_by_category(df, file_with_category):
    top_videos_by_ratio_json = {"categories": []}

    cur_df = df.filter(df.views > 100000). \
        withColumn("ratio", df.likes / df.dislikes). \
        sort(desc('ratio')). \
        coalesce(1).dropDuplicates(subset=['video_id'])
    categories = df.dropDuplicates(subset=['category_id']).select('category_id').collect()
    categories = [el[0] for el in categories]
    try:
        categories.remove("Screen Junkies")
    except:
        pass
    for category_id in categories:
        category_name = get_category_name(category_id, file_with_category)
        videos_df = cur_df.filter(cur_df.category_id == category_id).sort(desc('ratio')).limit(10)
        videos = []
        for video in videos_df.collect():
            videos.append({
                "video_id": video.video_id,
                "video_title": video.title,
                "ratio_likes_dislikes": video.ratio,
                "views": int(video.views)
            })
        category = {
            "category_id": int(category_id),
            "category_name": category_name,
            "videos": videos
        }

        top_videos_by_ratio_json["categories"].append(category)
    return top_videos_by_ratio_json


def process_data(df):
    category_file = "US_category_id.json"

    print("\n\nStart 1 question ")
    write_data(get_top_10_videos_by_days(df), "results/top_10_videos_by_days.json")
    print("Finish 1 question\n\n")

    print("\n\nStart 2 question ")
    write_data(get_top_categories_per_week(df, category_file), "results/top_categories_per_week.json")
    print("Finish 2 question\n\n")

    print("\n\nStart 3 question ")
    write_data(get_top10_tags(df), "results/top10_tags.json")
    print("Finish 3 question\n\n")

    print("\n\nStart 4 question ")
    write_data(get_top20_channels_by_views(df), "results/top20_channels_by_views.json")
    print("Finish 4 question\n\n")

    print("\n\nStart 5 question ")
    write_data(get_top10_channels_by_days(df), "results/top10_channels_by_days.json")
    print("Finish 5 question\n\n")

    print("\n\nStart 6 question ")
    write_data(get_top10_video_by_ratio_by_category(df, category_file), "results/top10_video_by_ratio_by_category.json")
    print("Finish 6 question\n\n")

    print("Well done. Good job!")


def main():
    df = read_data(SPARK, 'USvideos.csv')
    process_data(df)


if __name__ == '__main__':
    main()
