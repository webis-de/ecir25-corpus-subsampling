#!/usr/bin/env python3
import argparse
import requests
import time
import subprocess
from urllib.parse import urljoin
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
from os.path import exists


def __extract_links_for_selector_from_html_string(html_string, selector):
    soup = BeautifulSoup(html_string, 'html.parser')
    matches = soup.select(selector=selector)
    ret = [i['href'] for i in matches]
    ret.sort()

    return ret


def extract_navigation_links_from_html_string(html_string):
    return __extract_links_for_selector_from_html_string(
        html_string=html_string,
        selector='a[href*="./trec"]'
    ) + __extract_links_for_selector_from_html_string(
        html_string=html_string,
        selector='a[href*=".input"]'
    )


def extract_run_file_links_from_html_string(html_string):
    return __extract_links_for_selector_from_html_string(
        html_string=html_string,
        selector='a[href*="/input"]'
    )


def url_is_run(url):
    return input_url_directory(url) is not None


def input_url_directory(url):
    if url is None or not url.endswith('.gz'):
        return None

    for pattern in ['.gov/trec/', '.gov/results/']:
        if pattern in url:
            return '/'.join(url.split(pattern)[1].split('/')[0:-1])

    raise ValueError('Dont know how to proceed with: ' + url)


def output_file(url):
    return url.split('/')[-1]


def persist_run_file(url, args):
    result_dir = input_url_directory(url)
    if exists(output_file(url)):
        return
    
    cmd = [
        'bash',
        '-c',
        'mkdir -p results/' + result_dir
        + ' && cd results/' + result_dir + ' && curl --user ' + args.user + ':' + args.password + ' "'
        + url + '" --output ' + output_file(url)
    ]
    subprocess.check_output(cmd)


def crawl_url(url, args, url_frontier, visited_urls, sleep=5):
    print('Crawl ' + url)
    if sleep:
        time.sleep(sleep)
    visited_urls.add(url)

    if url_is_run(url):
        persist_run_file(url, args)
    else:
        response = requests.get(url, auth=HTTPBasicAuth(args.user, args.password))
        if response.status_code != 200:
            raise ValueError('-->' + str(response))
        response = response.content
        urls = extract_navigation_links_from_html_string(response) + extract_run_file_links_from_html_string(response)

        for out_url in set(urls):
            out_url = urljoin(url, out_url)
            if out_url not in visited_urls:
                url_frontier.append(out_url)

    while len(url_frontier) > 0:
        crawl_url(url_frontier.pop(), args, url_frontier, visited_urls)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Trec-System-Runs.')

    parser.add_argument('--password', type=str, required=True,
                        help='The password to access the protected area')
    parser.add_argument('--user', type=str, required=True,
                        help='The user to access the protected area')
                        
    parser.add_argument('--seed', type=str, required=True,
                        help='The seed url to start crawling runs from.')

    args = parser.parse_args()
    crawl_url(args.seed, args, [], set(), None)

    print('Done;)')
