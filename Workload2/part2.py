from pyspark import SparkContext
import argparse
import csv
import datetime

def extractData(record):
    parts = record.strip().split(",")
    likes,dislikes = parts[6],parts[7]
    like = float(likes)
    dislike = float(dislikes)
    trending_date = parts[1]
    video_id,country = parts[0],parts[11]
    category=parts[3]
    date =datetime.datetime.strptime(trending_date,'%y.%d.%m').timestamp()
    return ("{}|{}".format(video_id, country), (date,like, dislike,category))
    
def sort_and_calculate(inputs):
    sort_by_date = sorted(inputs, key = lambda x: x[0])
    first, second = sort_by_date[:2]
    dislike_increase = int(second[2]) - int(first[2])
    like_increase = int(second[1]) - int(first[1])
    dislike_growth = dislike_increase - like_increase
    return(dislike_growth, first[3])

def formateResult(x):
    video_id, country = x[0].split('|')
    dislike_growth, category = x[1]
    return '"{}", {}, "{}", "{}"'.format(video_id, dislike_growth, category, country)

if __name__ == "__main__":
    sc = SparkContext(appName="Controversial Trending Videos Identification")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='/home/hadoop/')
    parser.add_argument("--output", help="the output path",
                        default='task2_output')
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    videoData = sc.textFile(input_path+"AllVideos_short.csv")
    header='video_id,trending_date,category_id,category,publish_time,views,likes,dislikes,comment_count,ratings_disabled,video_error_or_removed,country'
    videoData = videoData.filter(lambda row: row != header)
    videoInfo = videoData.map(extractData)
    grouplist = videoInfo.groupByKey().filter(lambda x: len(x[1]) >= 2).mapValues(sort_and_calculate).sortBy(lambda x: x[1][0], ascending=False).take(10)
    
    formated_result = list(map(lambda x : formateResult(x), grouplist))
    sc.parallelize(formated_result).repartition(1).saveAsTextFile(output_path)
