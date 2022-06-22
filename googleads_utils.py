import sys
# sys.path.append('../') 
import simplejson as json
import os
import boto3
import botocore
from decimal import Decimal
import pprint
import urllib.parse
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import date, timedelta
import yamlreader
import s3_utils
from datetime import datetime
from pyathena import connect
import pandas as pd
import utils
import ddb_utils
import hashlib

def hashString(string):
    try:
        response = hashlib.md5(string.encode('utf-8')).hexdigest()
        return response
    except Exception as e:
        return e

config = utils.load_config("./")

def getToken(table_name, region, userId, personId, clientId, clientSecret):
    try:
        response = ddb_utils.getItem(config['aws']['keyId'], config['aws']['secretAccessKey'], config['googleads']['tableName'], config['dynamodb']['region'], config['googleads']['partKey'], userId)
        # print(response['Item']['payload']['token'])
        payload = response['Item']['payload']
        refresh_token = payload[personId]['token']['refresh_token']
        headers = {"client_id": clientId, "client_secret": clientSecret, "grant_type": "refresh_token", "refresh_token": refresh_token}
        url = "https://www.googleapis.com/oauth2/v4/token"
        r = r_session.post(url, json=headers)
        j = r.json()
        j['refresh_token'] = refresh_token
        # pprint.pprint(payload)
        obj = {}
        obj['token'] = j
        obj['personInfo'] = payload[personId]['personInfo']
        payload[personId] = obj
        ddb_utils.putItem(config['aws']['keyId'],config['aws']['secretAccessKey'],config['googleads']['tableName'], config['dynamodb']['region'], config['googleads']['partKey'], userId, 'payload', payload)
        token = j['access_token']
        return token
    except Exception as e:
        print("error getting token: {}".format(e))

def updateToken(userId, personId):
    try: 
        accessToken = getToken(config['googleads']['tableName'], config['dynamodb']['region'], userId, personId, config['googleads']['clientId'], config['googleads']['clientSecret'])
        headers={'Authorization': 'Bearer {}'.format(accessToken), 'login-customer-id': '{}'.format(config['googleads']['mccId']), 'developer-token': '{}'.format(config['googleads']['developerToken'])}
        r_session.headers.update(headers)
    except Exception as e:
        return e

def postQuery(query, accountId, pageSize, nextPageToken):
    try:
        url = 'https://googleads.googleapis.com/v8/customers/{}/googleAds:search'.format(accountId)
        print(url)
        payload = {"query": query, "pageSize": pageSize, "pageToken": nextPageToken}
        r = r_session.post(url, json=payload)
        j = r.json()
        # print(j)
        return j
    except Exception as e:
        response = {}
        response['error'] = e
        response['text'] = r.text
        return response

def getAdAccounts(userId, personId, mccId, fcontents, accountList, nextPageToken):
    updateToken(userId, personId)
    # sys.exit()
    query = 'SELECT customer_client.client_customer, customer_client.level, customer_client.manager, customer_client.descriptive_name, customer_client.currency_code, customer_client.time_zone, customer_client.id FROM customer_client WHERE customer_client.level > 0'
    try:
        fname = '{}:{}:match-tables-accounts'.format(hashString(personId), mccId)
        response = postQuery(query, mccId, config['googleads']['pageSize'], nextPageToken)
        items = response['results']
        if items:
            for item in items:
                isManager = item['customerClient']['manager']
                if isManager == False:
                    accountList.append(item['customerClient']['id'])
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), mccId, item['customerClient']['id'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getAdAccounts(personId, mccId, fcontents, accountList, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}match_tables/accounts/{}'.format(config['s3']['reports_prefix_ga'],fname))
            return accountList
        else:
            item = {}
            item['msg'] = 'no data'
            adAccountId = ''
            row = "{}\t{}\t{}\t{}\n".format(personId, mccId, adAccountId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}match_tables/accounts/{}'.format(config['s3']['reports_prefix_ga'],fname))
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getAdCampaigns(userId, personId, accountId, fcontents, nextPageToken):
    updateToken(userId, personId)    
    query = 'SELECT campaign.id, campaign.name, campaign.base_campaign, campaign.start_date, campaign.end_date, campaign.optimization_score, campaign.labels, campaign.campaign_budget, campaign.tracking_url_template, campaign.final_url_suffix, campaign.status FROM campaign'
    try:
        fname = '{}:{}:match-tables-campaigns'.format(hashString(personId), accountId)
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        items = response['results']
        # pprint.pprint(items)
        if items:
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['campaign']['id'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getAdCampaigns(personId, accountId, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}match_tables/campaigns/{}'.format(config['s3']['reports_prefix_ga'],fname))
        else:
            item = {}
            item['msg'] = 'no data'
            adCampaignId = ''
            row = "{}\t{}\t{}\t{}\n".format(userId, accountId, adCampaignId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}match_tables/campaigns/{}'.format(config['s3']['reports_prefix_ga'],fname))
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getAdGroups(userId, personId, accountId, fcontents, nextPageToken):
    updateToken(userId, personId)    
    query = 'SELECT ad_group.id, ad_group.name, ad_group.status FROM ad_group'
    try:
        fname = '{}:{}:match-tables-adgroups'.format(hashString(personId), accountId)
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        items = response['results']
        if items:
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['adGroup']['id'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getAdGroups(personId, accountId, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}match_tables/adgroups/{}'.format(config['s3']['reports_prefix_ga'],fname))
        else:
            item = {}
            item['msg'] = 'no data'
            adGroupId = ''
            row = "{}\t{}\t{}\t{}\n".format(userId, accountId, adGroupId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}match_tables/adgroups/{}'.format(config['s3']['reports_prefix_ga'],fname))
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getAds(userId, personId, accountId, fcontents, nextPageToken):
    updateToken(userId, personId)    
    query = 'SELECT ad_group_ad.policy_summary.policy_topic_entries, ad_group_ad.ad.final_url_suffix, ad_group_ad.ad.legacy_responsive_display_ad.accent_color, ad_group_ad.ad_group, ad_group_ad.ad_strength, ad_group_ad.ad.type, ad_group_ad.ad.legacy_responsive_display_ad.allow_flexible_color, ad_group_ad.ad.added_by_google_ads, ad_group_ad.ad.legacy_responsive_display_ad.business_name, ad_group_ad.ad.call_ad.phone_number, ad_group_ad.ad.legacy_responsive_display_ad.call_to_action_text, ad_group_ad.policy_summary.approval_status, ad_group_ad.ad.final_mobile_urls, ad_group_ad.ad.final_urls, ad_group_ad.ad.tracking_url_template, ad_group_ad.ad.url_custom_parameters, ad_group_ad.ad.legacy_responsive_display_ad.description, ad_group_ad.ad.expanded_text_ad.description, ad_group_ad.ad.text_ad.description1, ad_group_ad.ad.call_ad.description1, ad_group_ad.ad.text_ad.description2, ad_group_ad.ad.call_ad.description2, ad_group_ad.ad.device_preference, ad_group_ad.ad.display_url, ad_group_ad.ad.legacy_responsive_display_ad.logo_image, ad_group_ad.ad.legacy_responsive_display_ad.square_logo_image, ad_group_ad.ad.legacy_responsive_display_ad.marketing_image, ad_group_ad.ad.legacy_responsive_display_ad.square_marketing_image, ad_group_ad.ad.expanded_dynamic_search_ad.description, ad_group_ad.ad.expanded_text_ad.description2, ad_group_ad.ad.expanded_text_ad.headline_part3, ad_group_ad.ad.legacy_responsive_display_ad.format_setting, ad_group_ad.ad.gmail_ad.header_image, ad_group_ad.ad.gmail_ad.teaser.logo_image, ad_group_ad.ad.gmail_ad.marketing_image, ad_group_ad.ad.gmail_ad.teaser.business_name, ad_group_ad.ad.gmail_ad.teaser.description, ad_group_ad.ad.gmail_ad.teaser.headline, ad_group_ad.ad.text_ad.headline, ad_group_ad.ad.expanded_text_ad.headline_part1, ad_group_ad.ad.expanded_text_ad.headline_part2, ad_group_ad.ad.id, ad_group_ad.ad.image_ad.image_url, ad_group_ad.ad.image_ad.pixel_height, ad_group_ad.ad.image_ad.pixel_width, ad_group_ad.ad.image_ad.mime_type, ad_group_ad.ad.image_ad.name, ad_group_ad.ad.legacy_responsive_display_ad.long_headline, ad_group_ad.ad.legacy_responsive_display_ad.main_color, ad_group_ad.ad.gmail_ad.marketing_image_display_call_to_action.text, ad_group_ad.ad.gmail_ad.marketing_image_display_call_to_action.text_color, ad_group_ad.ad.gmail_ad.marketing_image_headline, ad_group_ad.ad.gmail_ad.marketing_image_description, ad_group_ad.ad.responsive_display_ad.accent_color, ad_group_ad.ad.responsive_display_ad.allow_flexible_color, ad_group_ad.ad.responsive_display_ad.business_name, ad_group_ad.ad.responsive_display_ad.call_to_action_text, ad_group_ad.ad.responsive_display_ad.descriptions, ad_group_ad.ad.responsive_display_ad.price_prefix, ad_group_ad.ad.responsive_display_ad.promo_text, ad_group_ad.ad.responsive_display_ad.format_setting, ad_group_ad.ad.responsive_display_ad.headlines, ad_group_ad.ad.responsive_display_ad.logo_images, ad_group_ad.ad.responsive_display_ad.square_logo_images, ad_group_ad.ad.responsive_display_ad.long_headline, ad_group_ad.ad.responsive_display_ad.main_color, ad_group_ad.ad.responsive_display_ad.marketing_images, ad_group_ad.ad.responsive_display_ad.square_marketing_images, ad_group_ad.ad.responsive_display_ad.youtube_videos, ad_group_ad.ad.expanded_text_ad.path1, ad_group_ad.ad.expanded_text_ad.path2, ad_group_ad.ad.legacy_responsive_display_ad.price_prefix, ad_group_ad.ad.legacy_responsive_display_ad.promo_text, ad_group_ad.ad.responsive_search_ad.descriptions, ad_group_ad.ad.responsive_search_ad.headlines, ad_group_ad.ad.responsive_search_ad.path1, ad_group_ad.ad.responsive_search_ad.path2, ad_group_ad.ad.legacy_responsive_display_ad.short_headline, ad_group_ad.status, ad_group_ad.ad.system_managed_resource_source, ad_group_ad.ad.app_ad.descriptions, ad_group_ad.ad.app_ad.headlines, ad_group_ad.ad.app_ad.html5_media_bundles, ad_group_ad.ad.app_ad.images, ad_group_ad.ad.app_ad.mandatory_ad_text, ad_group_ad.ad.app_ad.youtube_videos FROM ad_group_ad'
    try:
        fname = '{}:{}:match-tables-ads'.format(hashString(personId), accountId)
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        # pprint.pprint(response)
        if "results" in response:
            items = response['results']
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['adGroupAd']['ad']['id'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getAds(personId, accountId, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}match_tables/ads/{}'.format(config['s3']['reports_prefix_ga'],fname))
        else:
            item = {}
            item['msg'] = 'no data'
            adId = ''
            row = "{}\t{}\t{}\t{}\n".format(personId, accountId, adId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}match_tables/ads/{}'.format(config['s3']['reports_prefix_ga'],fname))
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getAdLabels(userId, personId, accountId, fcontents, nextPageToken):
    updateToken(userId, personId)    
    query = 'SELECT ad_group_ad.ad.id, label.resource_name, label.name FROM ad_group_ad_label'
    try:
        fname = '{}:{}:match-tables-ad-labels'.format(hashString(personId), accountId)
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        if "results" in response:
            items = response['results']
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['adGroupAd']['ad']['id'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getAdLabels(personId, accountId, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}match_tables/ad_labels/{}'.format(config['s3']['reports_prefix_ga'],fname))
        else:
            item = {}
            item['msg'] = 'no data'
            adId = ''
            row = "{}\t{}\t{}\t{}\n".format(personId, accountId, adId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}match_tables/ad_labels/{}'.format(config['s3']['reports_prefix_ga'],fname))
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getKwLabels(userId, personId, accountId, fcontents, nextPageToken):
    updateToken(userId, personId)    
    query = 'SELECT ad_group_criterion.criterion_id, label.resource_name, label.name FROM ad_group_criterion_label'
    try:
        fname = '{}:{}:match-tables-kw-labels'.format(hashString(personId), accountId)
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        # pprint.pprint(response)
        if "results" in response:
            items = response['results']
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['adGroupCriterion']['criterionId'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getKwLabels(personId, accountId, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}match_tables/kw_labels/{}'.format(config['s3']['reports_prefix_ga'],fname))
        else:
            item = {}
            item['msg'] = 'no data'
            kwId = ''
            row = "{}\t{}\t{}\t{}\n".format(personId, accountId, kwId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}match_tables/kw_labels/{}'.format(config['s3']['reports_prefix_ga'],fname))
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getKws(userId, personId, accountId, fcontents, nextPageToken):
    updateToken(userId, personId)    
    query = 'SELECT ad_group_criterion.criterion_id, ad_group_criterion.approval_status, ad_group_criterion.keyword.text, ad_group_criterion.final_mobile_urls, ad_group_criterion.final_url_suffix, ad_group_criterion.final_urls, ad_group_criterion.negative, ad_group_criterion.keyword.match_type, ad_group_criterion.status, ad_group_criterion.system_serving_status, ad_group_criterion.tracking_url_template, ad_group_criterion.url_custom_parameters, ad_group_criterion.topic.topic_constant FROM ad_group_criterion'
    try:
        fname = '{}:{}:match-tables-kws'.format(hashString(personId), accountId)
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        if "results" in response:
            items = response['results']
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['adGroupCriterion']['criterionId'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getKws(personId, accountId, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}match_tables/kws/{}'.format(config['s3']['reports_prefix_ga'],fname))
        else:
            item = {}
            item['msg'] = 'no data'
            kwId = ''
            row = "{}\t{}\t{}\t{}\n".format(personId, accountId, kwId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}match_tables/kws/{}'.format(config['s3']['reports_prefix_ga'],fname))
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getAudiences(userId, personId, accountId, fcontents, nextPageToken):
    updateToken(userId, personId)    
    query = 'SELECT ad_group_criterion.criterion_id, user_list.name FROM ad_group_audience_view'
    try:
        fname = '{}:{}:match-tables-audiences'.format(hashString(personId), accountId)
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        if "results" in response:
            items = response['results']
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['adGroupCriterion']['criterionId'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getAudiences(personId, accountId, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}match_tables/audiences/{}'.format(config['s3']['reports_prefix_ga'],fname))
        else:
            item = {}
            item['msg'] = 'no data'
            audId = ''
            row = "{}\t{}\t{}\t{}\n".format(personId, accountId, audId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}match_tables/audiences/{}'.format(config['s3']['reports_prefix_ga'],fname))
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getAdPerformanceReport(userId, personId, accountId, startDate, endDate, fname, fcontents, nextPageToken):
    updateToken(userId, personId) 
    query = "SELECT metrics.absolute_top_impression_percentage, metrics.active_view_impressions, metrics.active_view_measurability, metrics.active_view_measurable_cost_micros, metrics.active_view_measurable_impressions, metrics.active_view_viewability, ad_group_ad.ad_group, segments.ad_network_type, metrics.all_conversions_value, metrics.all_conversions, ad_group.base_ad_group, campaign.base_campaign, campaign.id, metrics.clicks, metrics.conversions_value, metrics.conversions, metrics.cost_micros, metrics.cross_device_conversions, metrics.current_model_attributed_conversions_value, metrics.current_model_attributed_conversions, segments.date, segments.device, metrics.engagements, customer.id, metrics.gmail_forwards, metrics.gmail_saves, metrics.gmail_secondary_clicks, ad_group_ad.ad.id, metrics.impressions, metrics.interaction_event_types, metrics.interactions, metrics.top_impression_percentage, metrics.video_quartile_p100_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p75_rate, metrics.video_views, metrics.view_through_conversions FROM ad_group_ad WHERE segments.date BETWEEN '{}' AND '{}'".format(startDate, endDate)
    try:
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        if "results" in response:
            items = response['results']
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['adGroupAd']['ad']['id'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getAdPerformanceReport(personId, accountId, startDate, endDate, fname, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'], fname)
        else:
            item = {}
            item['msg'] = 'no data'
            adId = ''
            row = "{}\t{}\t{}\t{}\n".format(personId, accountId, adId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'], fname)
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getKwPerformanceReport(userId, personId, accountId, startDate, endDate, fname, fcontents, nextPageToken):
    updateToken(userId, personId) 
    query = "SELECT metrics.absolute_top_impression_percentage, metrics.active_view_impressions, metrics.active_view_measurability, metrics.active_view_measurable_cost_micros, metrics.active_view_measurable_impressions, metrics.active_view_viewability, ad_group.id, segments.ad_network_type, metrics.all_conversions_value, metrics.all_conversions, ad_group.base_ad_group, campaign.base_campaign, campaign.id, metrics.clicks, metrics.conversions_value, metrics.conversions, metrics.cost_micros, ad_group_criterion.effective_cpc_bid_micros, ad_group_criterion.effective_cpc_bid_source, ad_group_criterion.effective_cpm_bid_micros, ad_group_criterion.quality_info.creative_quality_score, metrics.cross_device_conversions, metrics.current_model_attributed_conversions_value, metrics.current_model_attributed_conversions, segments.date, segments.device, metrics.engagements, ad_group_criterion.position_estimates.estimated_add_clicks_at_first_position_cpc, ad_group_criterion.position_estimates.estimated_add_cost_at_first_position_cpc, customer.id, ad_group_criterion.position_estimates.first_page_cpc_micros, ad_group_criterion.position_estimates.first_position_cpc_micros, metrics.gmail_forwards, metrics.gmail_saves, metrics.gmail_secondary_clicks, ad_group_criterion.criterion_id, metrics.impressions, metrics.interaction_rate, metrics.interaction_event_types, metrics.interactions, ad_group_criterion.quality_info.post_click_quality_score, ad_group_criterion.quality_info.quality_score, metrics.search_absolute_top_impression_share, metrics.search_budget_lost_absolute_top_impression_share, metrics.search_budget_lost_top_impression_share, metrics.search_exact_match_impression_share, metrics.search_impression_share, ad_group_criterion.quality_info.search_predicted_ctr, metrics.search_rank_lost_absolute_top_impression_share, metrics.search_rank_lost_impression_share, metrics.search_rank_lost_top_impression_share, metrics.search_top_impression_share, metrics.top_impression_percentage, ad_group_criterion.position_estimates.top_of_page_cpc_micros, metrics.video_quartile_p100_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p75_rate, metrics.video_views, metrics.view_through_conversions FROM keyword_view WHERE segments.date BETWEEN '{}' AND '{}'".format(startDate, endDate)
    try:
        response = postQuery(query, accountId, config['googleads']['pageSize'], nextPageToken)
        if "results" in response:
            items = response['results']
            for item in items:
                row = "{}\t{}\t{}\t{}\n".format(hashString(personId), accountId, item['adGroupCriterion']['criterionId'], json.dumps(item))
                fcontents += row
            if "nextPageToken" in response:
                nextPage = response['nextPageToken']
                getKwPerformanceReport(personId, accountId, startDate, endDate, fname, fcontents, nextPage)
            else:
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'], fname)
        else:
            item = {}
            item['msg'] = 'no data'
            kwId = ''
            row = "{}\t{}\t{}\t{}\n".format(personId, accountId, kwId, json.dumps(item))
            fcontents += row
            s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'], fname)
    except Exception as e:
        response = {}
        response['error'] = e
        # response['text'] = r.text
        return response

def getMatchTables(userId, personId, mccId):
    accountIds = getAdAccounts(userId, personId, mccId, '', [], '')
    pprint.pprint(accountIds)
    # sys.exit()
    # accountIds = ['7428541683']
    for accountId in accountIds:
        getAdCampaigns(userId, personId, accountId, '', '')
        getAdGroups(userId, personId, accountId, '', '')
        getAds(userId, personId, accountId, '', '')
        getAdLabels(userId, personId, accountId, '', '')
        getKws(userId, personId, accountId, '', '')
        getKwLabels(userId, personId, accountId, '', '')
        # print(getAudiences(userId, personId, accountId, '', ''))

def getReports(userId, personId, accountId, startDate, endDate, reportType, fname, attWindow):
    file = s3_utils.getFromS3(config['aws']['keyId'], config['aws']['secretAccessKey'], config['s3']['bucket'], fname)
    try:
        def days_between(d1, d2):
            d1 = datetime.strptime(d1, "%Y-%m-%d").date()
            # d2 = datetime.strptime(d2, "%Y-%m-%d")
            return abs((d2 - d1).days)
        print(startDate)
        lastMod = file.last_modified.date()
        check = days_between(endDate, lastMod)
        if check <= attWindow:
            print('date is within attribution window re-pulling report')
            if reportType == 'ads':
                getAdPerformanceReport(userId, personId, accountId, startDate, endDate, fname, '', '')
            if reportType == 'kws':
                getKwPerformanceReport(userId, personId, accountId, startDate, endDate, fname, '', '')
            if reportType == 'auds':
                getAudPerformanceReport(userId, personId, accountId, startDate, endDate, fname, '', '')
        else:
            print('date is outside of attribution window not re-pulling report')
    except Exception as e:
        print(e)
        print('file not found pulling report')
        try:
            if reportType == 'ads':
                getAdPerformanceReport(userId, personId, accountId, startDate, endDate, fname, '', '')
            if reportType == 'kws':
                getKwPerformanceReport(userId, personId, accountId, startDate, endDate, fname, '', '')
        except Exception as e:
            print(e)

def doGoogleAds(start, end):
    global r_session
    r_session = requests.Session()
    retry = Retry(connect=3, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    r_session.mount('http://', adapter)
    r_session.mount('https://', adapter)
    getMatchTables(config['googleads']['userId'], config['googleads']['personId'], config['googleads']['mccId'])
    delta = end - start
    accountIds = getAdAccounts(config['googleads']['userId'], config['googleads']['personId'], config['googleads']['mccId'], '', [], '')
    for accountId in accountIds:
        for i in range(delta.days + 1):
            date = str(start + timedelta(days=i))
            fname = '{}basic_performance_report/{}/{}:{}:{}:{}:{}'.format(config['s3']['reports_prefix_ga'],'ads',hashString(config['googleads']['personId']),accountId,'ads',date,date)
            getReports(config['googleads']['userId'], config['googleads']['personId'], accountId, date, date, 'ads', fname, config['googleads']['attWindow'])
            fname = '{}basic_performance_report/{}/{}:{}:{}:{}:{}'.format(config['s3']['reports_prefix_ga'],'kws',hashString(config['googleads']['personId']),accountId,'kws',date,date)
            getReports(config['googleads']['userId'], config['googleads']['personId'], accountId, date, date, 'kws', fname, config['googleads']['attWindow'])

    r_session.close()

if __name__ == '__main__':
    
    start = date(2020, 1, 1)
    end = date(2020, 12, 14)
    
    global r_session
    r_session = requests.Session()
    retry = Retry(connect=3, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    r_session.mount('http://', adapter)
    r_session.mount('https://', adapter)
    getMatchTables(config['googleads']['personId'], config['googleads']['mccId'])
    delta = end - start
    accountIds = getAdAccounts(config['googleads']['personId'], config['googleads']['mccId'], '', [], '')
    for accountId in accountIds:
        for i in range(delta.days + 1):
            date = str(start + timedelta(days=i))
            fname = '{}basic_performance_report/{}/{}:{}:{}:{}:{}'.format(config['s3']['reports_prefix_ga'],'ads',hashString(config['googleads']['personId']),accountId,'ads',date,date)
            getReports(config['googleads']['personId'], accountId, date, date, 'ads', fname, config['googleads']['attWindow'])
            fname = '{}basic_performance_report/{}/{}:{}:{}:{}:{}'.format(config['s3']['reports_prefix_ga'],'kws',hashString(config['googleads']['personId']),accountId,'kws',date,date)
            getReports(config['googleads']['personId'], accountId, date, date, 'kws', fname, config['googleads']['attWindow'])
    
    