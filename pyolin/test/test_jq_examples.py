"""
Examples from https://programminghistorian.org/en/lessons/json-and-jq

Data from https://programminghistorian.org/assets/jq_rkm.json
"""

import difflib

from pyolin.test.conftest import File, assert_contains, assert_startswith, custom_dedent


def test_print_reformatted(pyolin):
    """
    This tells jq to return the value of the field `artObjects`.

    jq '.artObjects'
    """
    output = pyolin(
        'jsonobj["artObjects"]',
        input_=File("data_jq_example_rkm.json"),
        output_format="json",
    ).getvalue()
    expected_prefix = custom_dedent(
        r"""
        [
          {
            "links": {
              "self": "https://www.rijksmuseum.nl/api/nl/collection/SK-C-5",
              "web": "https://www.rijksmuseum.nl/nl/collectie/SK-C-5"
            },
            "id": "nl-SK-C-5",
            "objectNumber": "SK-C-5",
            "title": "Schutters van wijk II onder leiding van kapitein Frans Banninck Cocq, bekend als de \u2018Nachtwacht\u2019",
            "hasImage": true,
            "principalOrFirstMaker": "Rembrandt Harmensz. van Rijn",
            "longTitle": "Schutters van wijk II onder leiding van kapitein Frans Banninck Cocq, bekend als de \u2018Nachtwacht\u2019, Rembrandt Harmensz. van Rijn, 1642",
            "showImage": true,
            "permitDownload": true,
            "webImage": {
              "guid": "3ae88fe0-021c-41ae-a4ce-cc70b7bc6295",
              "offsetPercentageX": 50,
        """  # noqa: E501
    )
    assert output.startswith(expected_prefix), "\n".join(
        difflib.unified_diff(
            output[: len(expected_prefix)].split("\n"), expected_prefix.split("\n")
        )
    )


def test_print_array(pyolin):
    """
    By adding [] onto the end of our filter, jq will break up this one array into 10 separate
    objects.

    jq '.artObjects[]'
    """
    assert_startswith(
        pyolin(
            'jsonobj["artObjects"]',
            input_=File("data_jq_example_rkm.json"),
            output_format="jsonl",
        ).getvalue(),
        r"""
        {"links": {"self": "https://www.rijksmuseum.nl/api/nl/collection/SK-C-5", "web": "https://www.rijksmuseum.nl/nl/collectie/SK-C-5"}, "id": "nl-SK-C-5", "objectNumber": "SK-C-5", "title": "Schutters van wijk II onder leiding van kapitein Frans Banninck Cocq, bekend als de \u2018Nachtwacht\u2019", "hasImage": true, "principalOrFirstMaker": "Rembrandt Harmensz. van Rijn", "longTitle": "Schutters van wijk II onder leiding van kapitein Frans Banninck Cocq, bekend als de \u2018Nachtwacht\u2019, Rembrandt Harmensz. van Rijn, 1642", "showImage": true, "permitDownload": true, "webImage": {"guid": "3ae88fe0-021c-41ae-a4ce-cc70b7bc6295", "offsetPercentageX": 50, "offsetPercentageY": 100, "width": 2500, "height": 2034, "url": "http://lh6.ggpht.com/ZYWwML8mVFonXzbmg2rQBulNuCSr3rAaf5ppNcUc2Id8qXqudDL1NSYxaqjEXyDLSbeNFzOHRu0H7rbIws0Js4d7s_M=s0"}, "headerImage": {"guid": "29a2a516-f1d2-4713-9cbd-7a4458026057", "offsetPercentageX": 50, "offsetPercentageY": 50, "width": 1920, "height": 460, "url": "http://lh3.ggpht.com/rvCc4t2BWHAgDlzyiPlp1sBhc8ju0aSsu2HxR8rN_ZVPBcujP94pukbmF3Blmhi-GW5cx1_YsYYCDMTPePocwM6d2vk=s0"}, "productionPlaces": ["Amsterdam"]}
        {"links": {"self": "https://www.rijksmuseum.nl/api/nl/collection/SK-A-1505", "web": "https://www.rijksmuseum.nl/nl/collectie/SK-A-1505"}, "id": "nl-SK-A-1505", "objectNumber": "SK-A-1505", "title": "Een molen aan een poldervaart, bekend als \u2018In de maand juli\u2019", "hasImage": true, "principalOrFirstMaker": "Paul Joseph Constantin Gabri\u00ebl", "longTitle": "Een molen aan een poldervaart, bekend als \u2018In de maand juli\u2019, Paul Joseph Constantin Gabri\u00ebl, ca. 1889", "showImage": true, "permitDownload": true, "webImage": {"guid": "85747e15-0b95-4306-922f-0fa17fc0ffeb", "offsetPercentageX": 50, "offsetPercentageY": 50, "width": 1767, "height": 2748, "url": "http://lh4.ggpht.com/PkQr-nNqzn0OVXVd4-hdJ6PPdWZ6-DQ_74WfBT3MZIV4LNYA-q8LUrtReXNstuzl9k6gKWkaBwG-LcFZ7zWU9Ch92g=s0"}, "headerImage": {"guid": "34e94d7f-4d7a-464e-b3f9-eb5532f98c27", "offsetPercentageX": 50, "offsetPercentageY": 50, "width": 1920, "height": 460, "url": "http://lh3.ggpht.com/1JKghbAAi6kmGGW1QeVJpeBdarUXrZwtwKx9Y94fqjnB2Ln5keXAG02ZDazKH0qrVqi7QWDfm0SfEIWBP1xru_6Cyg=s0"}, "productionPlaces": []}
        """,  # noqa: E501
    )


def test_pipe(pyolin):
    """
    This will return a list of every value at the key id within the artObjects array, separated by a
    line break.

    jq '.artObjects[] | .id'
    """
    output = pyolin(
        'obj["id"] for obj in jsonobj["artObjects"]',
        input_=File("data_jq_example_rkm.json"),
        output_format="awk",
    )
    assert output == (
        """\
        nl-SK-C-5
        nl-SK-A-1505
        nl-SK-A-180
        nl-SK-A-2205
        nl-SK-A-1923
        nl-SK-A-1935
        nl-SK-A-690
        nl-SK-A-2983
        nl-SK-A-3924
        nl-SK-A-3246
        """
    )


def test_select(pyolin):
    """
    Normally jq repeats every filter operation for each line of input that it receives, passing each
    answer on to the following filter operation. select() will only pass on a subset of the input
    onto the next step of the filter.

    jq '.artObjects[] | select(.productionPlaces | length >= 1) | .id'
    """
    output = pyolin(
        'obj["id"] for obj in jsonobj["artObjects"] if len(obj["productionPlaces"]) >= 1',
        input_=File("data_jq_example_rkm.json"),
        output_format="awk",
    )
    assert output == (
        """\
        nl-SK-C-5
        nl-SK-A-3924
        """
    )


def test_string_match(pyolin):
    """
    jq can also filter based on regular expressions. For example, let's select
    only those objects whose primary maker has the particle "van" in their name,
    and return the artist name and artwork id. `test("van")` takes the value
    returned by the operator `.principalOrFirstMaker` and returns true if that
    value contains the string van:

    jq '.artObjects[]
        | select(.principalOrFirstMaker
        | test("van"))
        | {id: .id, artist: .principalOrFirstMaker}'
    """
    output = pyolin(
        '{"id": obj["id"], "artist": obj["principalOrFirstMaker"]}'
        ' for obj in jsonobj["artObjects"] if re.search(r"van", obj["principalOrFirstMaker"])',
        input_=File("data_jq_example_rkm.json"),
        output_format="jsonl",
    )
    assert output == (
        """\
        {"id": "nl-SK-C-5", "artist": "Rembrandt Harmensz. van Rijn"}
        {"id": "nl-SK-A-180", "artist": "Gerard van Honthorst"}
        {"id": "nl-SK-A-2205", "artist": "Gerrit van Vucht"}
        {"id": "nl-SK-A-1935", "artist": "Rembrandt Harmensz. van Rijn"}
        {"id": "nl-SK-A-3246", "artist": "Adriaen van Ostade"}
        """
    )


def test_create_new_object(pyolin):
    """
    By wrapping . operators within either [] or {}, jq can synthesize new JSON
    arrays and objects. This can be useful if you want to output a new JSON
    file. As we will see below, this can also be a crucial intermediate step
    when reshaping complex JSON.

    jq '.artObjects[] | {id: .id, title: .title}'
    """
    output = pyolin(
        '{"id": obj["id"], "title": obj["title"]} for obj in jsonobj["artObjects"]',
        input_=File("data_jq_example_rkm.json"),
        output_format="jsonl",
    )
    assert_startswith(
        output.getvalue(),
        r"""
        {"id": "nl-SK-C-5", "title": "Schutters van wijk II onder leiding van kapitein Frans Banninck Cocq, bekend als de \u2018Nachtwacht\u2019"}
        {"id": "nl-SK-A-1505", "title": "Een molen aan een poldervaart, bekend als \u2018In de maand juli\u2019"}
        """,  # noqa: E501,
    )


def test_create_array(pyolin):
    """
    We can also create arrays using []:

    jq '.artObjects[] | [.id, .title]'
    """
    output = pyolin(
        'json.dumps([obj["id"], obj["title"]]) for obj in jsonobj["artObjects"]',
        input_=File("data_jq_example_rkm.json"),
        output_format="awk",
    )
    assert_startswith(
        output.getvalue(),
        r"""
        ["nl-SK-C-5", "Schutters van wijk II onder leiding van kapitein Frans Banninck Cocq, bekend als de \u2018Nachtwacht\u2019"]
        ["nl-SK-A-1505", "Een molen aan een poldervaart, bekend als \u2018In de maand juli\u2019"]
        """,  # noqa: E501,
    )


def test_output_csv(pyolin):
    """
    To create a CSV table with jq we want to filter our input JSON into a series
    of arrays, with each array being a row of the CSV.

    jq '.artObjects[]
        | [.id, .title, .principalOrFirstMaker, .webImage.url]
        | @csv'
    """
    output = pyolin(
        '(obj["id"], obj["title"], obj["principalOrFirstMaker"], obj["webImage"]["url"])'
        ' for obj in jsonobj["artObjects"]',
        input_=File("data_jq_example_rkm.json"),
        output_format="csv",
    )
    assert_startswith(
        output.getvalue(),
        """\
        nl-SK-C-5,"Schutters van wijk II onder leiding van kapitein Frans Banninck Cocq, bekend als de ‘Nachtwacht’",Rembrandt Harmensz. van Rijn,http://lh6.ggpht.com/ZYWwML8mVFonXzbmg2rQBulNuCSr3rAaf5ppNcUc2Id8qXqudDL1NSYxaqjEXyDLSbeNFzOHRu0H7rbIws0Js4d7s_M=s0\r
        nl-SK-A-1505,"Een molen aan een poldervaart, bekend als ‘In de maand juli’",Paul Joseph Constantin Gabriël,http://lh4.ggpht.com/PkQr-nNqzn0OVXVd4-hdJ6PPdWZ6-DQ_74WfBT3MZIV4LNYA-q8LUrtReXNstuzl9k6gKWkaBwG-LcFZ7zWU9Ch92g=s0\r
        """,  # noqa: E501,
    )


# Note: The dataset is truncated for testing performance. As such, the count (last 3 test cases)
# values are different from the source lesson.


def test_one_row_per_tweet(pyolin):
    """
    Let's create a table with one column with a tweet ID, and a second column
    with all the hashtags in each tweet, separated by a semicolon: `;`

    jq '{id: .id, hashtags: .entities.hashtags}'
    """
    output = pyolin(
        "{'id': jsonobj['id'], 'hashtags': jsonobj['entities']['hashtags']}",
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="jsonl",
    )
    assert_startswith(
        output.getvalue(),
        """\
        {"id": 501064141332029440, "hashtags": [{"indices": [41, 50], "text": "Ferguson"}]}
        {"id": 501064171707170816, "hashtags": [{"indices": [139, 140], "text": "Ferguson"}]}
        """,
    )


def test_text_of_hashtags(pyolin):
    """
    The value of hashtags is the array (wrapped in []) from the original data,
    which may have 0 or more objects inside it. Let's add a second query to
    preserve just the text of those hashtags:

    jq '{id: .id, hashtags: .entities.hashtags}
        | {id: .id, hashtags: .hashtags[].text}'

    A fun note: There is a bug in some versions of jq
    (https://github.com/jqlang/jq/issues/1959) that makes the large numeric IDs
    lose precision / return altered values after going through jq. The example
    data just happens to have one of those large numeric ID values, which means
    the outputs shown in the blog post are actually buggy.

    Python is parsing these numbers as integers, not floats, so it preserves
    these large values.
    """
    output = pyolin(
        "{'id': obj['id'], 'hashtags': hashtag['text']} for obj in jsonobjs for hashtag in obj['entities']['hashtags']",  # noqa: E501
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="jsonl",
    )
    assert_startswith(
        output.getvalue(),
        """\
        {"id": 501064141332029440, "hashtags": "Ferguson"}
        {"id": 501064171707170816, "hashtags": "Ferguson"}
        {"id": 501064180468682752, "hashtags": "Ferguson"}
        {"id": 501064194309906436, "hashtags": "USNews"}
        {"id": 501064196931330049, "hashtags": "Ferguson"}
        {"id": 501064196931330049, "hashtags": "MikeBrown"}
        """,
    )


def test_delimiting_with_semicolon(pyolin):
    """
    Finally, we want to express this as a CSV file, delimiting the hashtags with `;`.

    jq '{id: .id, hashtags: .entities.hashtags}
        | {id: .id, hashtags: [.hashtags[].text]}
        | {id: .id, hashtags: .hashtags | join(";")}'
    """
    output = pyolin(
        "{'id': jsonobj['id'], 'hashtags': ';'.join(ht['text'] for ht in jsonobj['entities']['hashtags'])}",  # noqa: E501
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="jsonl",
    )
    assert_startswith(
        output.getvalue(),
        """\
        {"id": 501064141332029440, "hashtags": "Ferguson"}
        {"id": 501064171707170816, "hashtags": "Ferguson"}
        {"id": 501064180468682752, "hashtags": "Ferguson"}
        {"id": 501064188211765249, "hashtags": ""}
        {"id": 501064194309906436, "hashtags": "USNews"}
        {"id": 501064196931330049, "hashtags": "Ferguson;MikeBrown"}
        """,
    )


def test_twitter_output_csv(pyolin):
    """
    Now, we can finally format the individual rows of the CSV and output it

    jq '{id: .id, hashtags: .entities.hashtags}
        | {id: .id, hashtags: [.hashtags[].text]}
        | {id: .id, hashtags: .hashtags | join(";")}
        | [.id, .hashtags]
        | @csv'
    """
    output = pyolin(
        "jsonobj['id'], ';'.join(ht['text'] for ht in jsonobj['entities']['hashtags'])",
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="csv",
    )
    assert_startswith(
        output.getvalue(),
        """\
        501064141332029440,Ferguson\r
        501064171707170816,Ferguson\r
        501064180468682752,Ferguson\r
        501064188211765249,\r
        501064194309906436,USNews\r
        501064196931330049,Ferguson;MikeBrown\r
        """,
    )


def test_one_row_per_hashtag(pyolin):
    """
    jq '{id: .id, hashtags: .entities.hashtags}
        | {id: .id, hashtag: .hashtags[].text}
        | [.id, .hashtag]
        | @csv'
    """
    output = pyolin(
        "(obj['id'], ht['text']) for obj in jsonobjs for ht in obj['entities']['hashtags']",
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="csv",
    )
    assert_startswith(
        output.getvalue(),
        """\
        501064141332029440,Ferguson\r
        501064171707170816,Ferguson\r
        501064180468682752,Ferguson\r
        501064194309906436,USNews\r
        501064196931330049,Ferguson\r
        501064196931330049,MikeBrown\r
        """,
    )


def test_group_by_user(pyolin):
    """
    We can use group_by(.user) to collect these tweets into sub-arrays of one user each.

    jq -s 'group_by(.user)'
    """
    output = pyolin(
        (
            "d = collections.defaultdict(list);"
            "[d[obj['user']['id']].append(obj) for obj in jsonobjs];"
            "d.values()"
        ),
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="json",
    )
    assert_startswith(
        output.getvalue(),
        r"""
        [
          [
            {
              "contributors": null,
              "truncated": false,
              "text": "Gobernador dice que el toque de queda en #Ferguson por disturbios raciales podr\u00eda durar d\u00edas http://t.co/doAajeQkom",
              "is_quote_status": false,
              "in_reply_to_status_id": null,
        """,  # noqa: E501
    )


def test_create_table_of_users(pyolin):
    """
    We can now create a table of users. Let's create a table with columns for the user id, user
    name, followers count, and a column of their tweet ids separated by a semicolon.

    jq 'group_by(.user)
        | .[]
        | {
            user_id: .[0].user.id,
            user_name: .[0].user.screen_name,
            user_followers: .[0].user.followers_count,
            tweet_ids: [.[].id | tostring] | join(";")
          }
        | [.user_id, .user_name, .user_followers, .tweet_ids]
        | @csv'
    """
    output = pyolin(
        (
            "d = collections.defaultdict(list);"
            "[d[obj['user']['id']].append(obj) for obj in jsonobjs];"
            "(id, tweets[0]['user']['screen_name'], tweets[0]['user']['followers_count'], "
            "';'.join(str(t['id']) for t in tweets)) for id, tweets in d.items()"
        ),
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="csv",
    )
    assert_startswith(
        output.getvalue(),
        # Expected values are not the same as in the guide, because the group_by order is different.
        """
        851336634,20mUsa,15643,501064141332029440\r
        53158947,MzDivah67,5661,501064171707170816\r
        619587350,BrookLyn1825,1208,501064180468682752\r
        374346913,I_Mpower,3390,501064188211765249\r
        2272978051,Vorarlberg1,490,501064194309906436\r
        278298244,bookishshelly,186,501064196931330049\r
        1112443196,deegerwiilen,1356,501064197396914176\r
        21811025,mmaureen7,1705,501064197632167936\r
        """,
    )


def test_count_hashtags(pyolin):
    """
    In this final exercise, we will use jq to count the number of times unique hashtags appear in
    this dataset.

    jq -s '[
        .[]
        | {id: .id, hashtag: .entities.hashtags}
        | {id: .id, hashtag: .hashtag[].text}
    ] | group_by(.hashtag)
      | .[]
      | {tag: .[0].hashtag, count: . | length}
      | [.tag, .count]
      | @csv'
    """
    output = pyolin(
        "collections.Counter(ht['text'] for obj in jsonobjs for ht in obj['entities']['hashtags']).items()",  # noqa: E501
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="csv",
    )
    assert_startswith(
        output.getvalue(),
        # Expected values are not the same as in the guide, because the group_by order is different.
        """
        Ferguson,8\r
        USNews,1\r
        MikeBrown,1\r
        tcot,1\r
        uniteblue,1\r
        teaparty,1\r
        gop,1\r
        PoliceBrutality,1\r
        """,
    )


# === Challenges ===


def test_filter_before_counting(pyolin):
    """
    What function do we need to add to the hashtag-counting filter to only count hashtags when their
    tweet has been retweeted at least 200 times? Hint: the retweet count is saved under the key
    `retweet_count`.
    """
    output = pyolin(
        "collections.Counter(ht['text'] for obj in jsonobjs for ht in obj['entities']['hashtags'] if obj['retweet_count'] >= 200).items()",  # noqa: E501
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="csv",
    )
    assert_startswith(
        output.getvalue(),
        """
        Ferguson,1\r
        MikeBrown,1\r
        """,
    )


def test_count_total_tweets_per_user(pyolin):
    """
    One more challenge to test your mastery of jq: from this dataset, try to compute the total
    number of times each user has had their tweets (at least within this dataset) retweeted.
    """
    output = pyolin(
        (
            "d = collections.defaultdict(list);"
            "[d[obj['user']['id']].append(obj) for obj in jsonobjs];"
            "(id, sum(t['retweet_count'] for t in tweets)) for id, tweets in d.items()"
        ),
        input_=File("data_jq_example_twitter.jsonl"),
        output_format="csv",
    )
    assert_contains(
        output.getvalue(),
        """
        278298244,225\r
        """,
    )
