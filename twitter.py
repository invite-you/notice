from math import fabs
import requests
import os
import json

from ratelimit import limits, sleep_and_retry

from loguru import logger

import re
import datetime

from googletrans import Translator
import telegram

import time

logger.add("tweet_trans_{time}.log", rotation="1 week")

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = r"AAAAAAAAAAAAAAAAAAAAAPQzbQEAAAAAl2V1uePmjnqKre%2B423hWmiwUnCc%3DQhrpyUDovTPxkusIpXq3VGjAHboWn3SiKhO37IdB3M139TJb1o"#os.environ.get("BEARER_TOKEN")
bearer_token = r"AAAAAAAAAAAAAAAAAAAAADaKfQEAAAAA7Bgn9%2BM77JknqzN3G0Ai88zy%2BnE%3DapXgghjLIERF6Ms8x5lQiAEWQNB4RIN5bchS03ZTFLoJVpFoNJ"
#bearer_token = os.environ.get("BEARER_TOKEN")

CALL = 300
SECOND = 900

@sleep_and_retry
@limits(calls=CALL, period=SECOND)
def connect_to_endpoint(url, params: dict, method="GET", stream=False):
    if method == "GET":
        if stream == False:
            response = requests.request("GET", url, auth=bearer_oauth, params=params)
        else:
            response = requests.request("GET", url, auth=bearer_oauth, params=params, stream=True)
    else:
        response = requests.request("POST", url, auth=bearer_oauth, json=params)
    
    if not ((response.status_code != 200) or (response.status_code != 201)):
        logger.error(response.status_code)
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r

def get_tweets(tweet_ids:list):                
    url = "https://api.twitter.com/2/tweets?ids={}".format(",".join(tweet_ids))
    params = {"tweet.fields": "created_at,referenced_tweets,lang", 
            "expansions": "author_id", 
            "user.fields":"name"}        
    res = connect_to_endpoint(url, params, method="GET").json()
    logger.debug(json.dumps(res))
    return res

def get_tweet(tweet_id:str):                
    url = "https://api.twitter.com/2/tweets/{}".format(tweet_id)
    params = {"tweet.fields": "created_at,referenced_tweets,lang", 
            "expansions": "author_id", 
            "user.fields":"name"}      
    res = connect_to_endpoint(url, params, method="GET").json()
    logger.debug(json.dumps(res))
    return res

def get_rules():
    url = "https://api.twitter.com/2/tweets/search/stream/rules"
    params = {}
    response = connect_to_endpoint(url, params, method="GET").json()
    logger.debug(json.dumps(response))
    return response

def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None
    url = "https://api.twitter.com/2/tweets/search/stream/rules"
    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = connect_to_endpoint(url, payload, method="POST").json()
    logger.debug(json.dumps(response))
    return response
    

def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "from:GyeongminKim16"},
        {"value": "from:WalterBloomberg"},
        {"value": "from:financialjuice"},
        {"value": "from:Sino_Market"},
    ]
    url = "https://api.twitter.com/2/tweets/search/stream/rules"
    payload = {"add": sample_rules}
    response = connect_to_endpoint(url, payload, method="POST").json()
    logger.debug(json.dumps(response))
    return response

# disable
def specify_fields():    
    params =  {"tweet.fields": "created_at,referenced_tweets,lang", 
            "expansions": "author_id", 
            "user.fields":"name"} 
              
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream",
        auth=bearer_oauth,
        params=params,
        stream=True,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream():
    params =  {"tweet.fields": "created_at,referenced_tweets,lang", 
                "expansions": "author_id", 
                "user.fields":"name"} 
    url = "https://api.twitter.com/2/tweets/search/stream"
    stream_response = connect_to_endpoint(url, params, method="GET", stream=True)
    return stream_response



def find_user_nickname(users, guess_user_id):
    """
    return
    {'id': '1549663522170101760', 'name': 'Gyeongmin Kim', 'username': 'GyeongminKim16'}
    """
    return [user for user in users if guess_user_id == user['id']][0]['name']


def remove_text_url(text):
    return re.sub(' https://t.co/.*$', '',text)


def make_tweet_info_message(tweet, users, include_link=False):
    nickname, date = '', ''
    # 트윗의 생성시간
    if 'created_at' in tweet:
        if isinstance(tweet['created_at'], str):
            tweet['created_at'] = datetime.datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
        date = tweet['created_at'].strftime("%m/%d_%H:%M")
    # 트윗한 사람의 닉네임
    if 'author_id' in tweet:
        nickname = '*' + find_user_nickname(users, tweet['author_id'])
    else:
        nickname = users[0]['name']
    # 트윗 링크
    if include_link:            
        link = f"https://twitter.com/{tweet['author_id']}/status/{tweet['id']}"
        return f"{nickname}<a href='{link}'>(link)</a> {date}"
    return f"{nickname} {date}\n"


def get_referenced_tweets_message(tweet, max_count=3):
    if 0 == max_count:
        return ''
    # 레퍼런스 트윗 가져오기
    if 'referenced_tweets' in tweet:
        ref_tweet_id = tweet['referenced_tweets'][0]['id']
        ref_tweet_raw = get_tweet(ref_tweet_id)
        ref_users = ref_tweet_raw['includes']['users']   
        ref_tweet = ref_tweet_raw['data']
        # 메시지 만들기
        ref_message_title = make_tweet_info_message(ref_tweet, ref_users)
        ref_message_content = remove_text_url(ref_tweet['text'])
        ref_message = f'\n┗ {ref_message_title}{ref_message_content}'
        # 재귀로 리트윗을 가져옴
        remained_ref_tweet = get_referenced_tweets_message(ref_tweet, max_count=max_count-1)
        ref_message += remained_ref_tweet
        return ref_message
    return ''


def skip_message(text):
    skip_words = ["$MACRO"]
    for skip_word in skip_words:
        if skip_word in text:
            return True
    return False


def main():
    translator = Translator()
    bot = telegram.Bot(token='5544440988:AAF35TV4c5A14VKrbcSk6xpmCQjO61ec-pE')

    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    #specify_fields()

    for _ in range(10):
        logger.debug("START: Streaming")
        stream_response = get_stream()
        try:
            # 인코딩 변경
            if stream_response.encoding is None:
                stream_response.encoding = 'utf-8'
            for tweet_raw in stream_response.iter_lines(decode_unicode=True):
                # 트윗이 발생
                if tweet_raw:
                    tweet_raw = json.loads(tweet_raw)
                    logger.debug("Tweet raw: {}".format(tweet_raw))
                    # users = [{'id': '1549663522170101760', 'name': 'Gyeongmin Kim', 'username': 'GyeongminKim16'}, {'id': '34442404', 'name': 'Sony', 'username': 'Sony'}]
                    users = tweet_raw['includes']['users']        
                    # tweet = {'author_id': '1549663522170101760', 'created_at': '2022-07-30T09:48:13.000Z', 'id': '1553316524013654016', 'referenced_tweets': [{'type': 'quoted', 'id': '1551960654604558336'}], 'text': 'ㄴㄷㄹㄴㄷㄹㄴㄷㄹ\n안녕하세요\n방가바강 https://t.co/vaHKOyq5jg'}
                    tweet = tweet_raw['data'] 
                    #불필요한 메시지일 경우 제외
                    if skip_message(tweet['text']):
                        continue
                    message_title = make_tweet_info_message(tweet, users, include_link=True)
                    message_content = remove_text_url(tweet['text'])
                    message_ref_tweet = get_referenced_tweets_message(tweet)

                    message = f"{message_title}\n{message_content}{message_ref_tweet}"
                    logger.debug("Message: {}".format(message))

                    if 'en' == tweet['lang'] or 'qst' == tweet['lang'] :
                        #message_trans = f"{message_title}\n{message_content}. {message_ref_tweet}"
                        message_trans = f"{message_content} {message_ref_tweet}"
                        message_trans_ko = translator.translate(message_trans, src='en', dest='ko').text
                        message_ko = f"{message_title}\n{message_trans_ko}"
                        logger.debug("KOR Message: {}".format(message_ko))
                        message_send = f"{message_ko}\n\n(EN){message}"
                        logger.info(message_send)
                        bot.sendMessage(chat_id = '-1001425744767', text=message_send, parse_mode=telegram.ParseMode.HTML, disable_web_page_preview=True)
                    else:
                        bot.sendMessage(chat_id = '-1001425744767', text=message, parse_mode=telegram.ParseMode.HTML, disable_web_page_preview=True)
        except Exception:
            logger.exception("Error:", exc_info=True)
            stream_response.close()
            time.sleep(5)

if __name__ == "__main__":
    main()


    """
            #{"value": "dog has:images", "tag": "dog pictures"},
        #{"value": "cat has:images -grumpy", "tag": "cat pictures"},"""
