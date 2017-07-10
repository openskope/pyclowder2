#!/usr/bin/env python
import os
import argparse
import json
import requests
from pyclowder.clowder import Clowder


def dataset_list(args, clowder):

    dataset_list = clowder.list_datasets()
    for data in dataset_list:
        print(data['name']) 


def dataset_add(args, clowder):

    clowder.create_dataset(args.dataset_name)


def dataset_delete(args, clowder):

    clowder.delete_dataset_name(args.dataset_name)


def file_list(args, clowder):

    dataset_id = get_dataset_id(args.dataset_name)
    file_list = get_file_list(None, host, key, dataset_id)
    for f in file_list:
        print(f['filename'])


def file_add(args, clowder):

    dataset_id = clowder.get_dataset_id(args.dataset_name)
    if args.local is not None:
        pass
    else:
        upload_to_dataset(None, host, key, dataset_id,
                          args.file_name, True)


def file_delete(args, clowder):

    dataset_id = clowder.get_dataset_id(args.dataset_name)
    file_id = get_file_id(dataset_id, args.file_name)
    requests.delete(API + 'datasets/' + dataset_id + '/' +\
                    file_id + '?key=' + key)


def setup_commands(parser):

    parser.add_argument('--clowder-url', dest='url',
            default=os.environ.get('CLOWDER_HOST', ''),
            help='URL of Clowder host (Env: CLOWDER_URL)')
    parser.add_argument('--clowder-login', dest='login',
            default=os.environ.get('CLOWDER_LOGIN', ''),
            help='Clowder login (Env: CLOWDER_LOGIN)')
    parser.add_argument('--clowder-password', dest='password',
            default=os.environ.get('CLOWDER_PASSWORD', ''),
            help='Clowder password (Env: CLOWDER_PASSWORD)')
    subparsers = parser.add_subparsers()

    #datasets
    dataset = subparsers.add_parser('dataset')
    subp = dataset.add_subparsers()
    
    subcmd = subp.add_parser('list', help='list all datasets')
    subcmd.add_argument('--space', type=str)
    subcmd.add_argument('--collection', type=str)
    subcmd.set_defaults(func=dataset_list)

    subcmd = subp.add_parser('add')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.set_defaults(func=dataset_add)
 
    subcmd = subp.add_parser('delete')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.set_defaults(func=dataset_delete)

    #file
    File = subparsers.add_parser('file')
    subp = File.add_subparsers()

    subcmd = subp.add_parser('list', help='list all files in a dataset')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.set_defaults(func=file_list)

    subcmd = subp.add_parser('add')
    subcmd.add_argument('--local', default=False, action='store_true')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.add_argument('file_name', type=str)
    subcmd.set_defaults(func=file_add)

    subcmd = subp.add_parser('delete')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.add_argument('file_name', type=str)
    subcmd.set_defaults(func=file_delete)


def main():

    parser = argparse.ArgumentParser()
    setup_commands(parser) 
    args = parser.parse_args()

    clowder = Clowder(url=args.url, auth=(args.login, args.password))
    args.func(args, clowder)
    
        
if __name__ == '__main__':
    main()
