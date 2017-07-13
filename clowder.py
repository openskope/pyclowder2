#!/usr/bin/env python
import os
import argparse
import json
import requests
import sys
from pprint import pprint
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

    file_list = clowder.list_file(args.dataset_name)
    #print(file_list)
    for f in file_list:
        print(f['filename'])


def file_add(args, clowder):

    if not args.local:
        clowder.add_file(args.dataset_name, args.file_name)
    else:
        pass


def file_delete(args, clowder):

    clowder.delete_file(args.dataset_name, args.file_name)


def dataset_metadata_list(args, clowder):

    metadata = clowder.list_dataset_metadata(args.dataset_name)

    pprint(metadata)


def dataset_metadata_add(args, clowder):

    metadata = {
        '@context':['https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld'],
        'agent':{'@type':'cat:user',
                 'user_id':''},
        'content':{},
    }
    for meta_str in args.meta_strs:
        key_value = meta_str.split('=')
        metadata['content'][key_value[0]]=key_value[1]

    clowder.add_dataset_metadata(args.dataset_name, metadata)


def dataset_metadata_delete(args, clowder):

    clowder.delete_dataset_metadata(args.dataset_name)


def file_metadata_list(args, clowder):

    metadata = clowder.list_file_metadata(args.dataset_name,
                                          args.file_name)

    pprint(metadata)


def file_metadata_add(args, clowder):

    metadata = {
        '@context':['https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld'],
        'agent':{'@type':'cat:user',
                 'user_id':''},
        'content':{},
    }
    for meta_str in args.meta_strs:
        key_value = meta_str.split('=')
        metadata['content'][key_value[0]]=key_value[1]

    clowder.add_file_metadata(args.dataset_name, 
                              args.file_name, metadata)


def file_metadata_delete(args, clowder):

    clowder.delete_file_metadata(args.dataset_name,
                                 args.file_name)


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

    #dataset metadata
    metadata = subp.add_parser('metadata')
    metadata_ops = metadata.add_subparsers()

    subcmd = metadata_ops.add_parser('list')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.set_defaults(func=dataset_metadata_list)

    subcmd = metadata_ops.add_parser('add')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.add_argument('meta_strs', type=str, nargs='*')
    subcmd.set_defaults(func=dataset_metadata_add)

    subcmd = metadata_ops.add_parser('delete')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.set_defaults(func=dataset_metadata_delete)

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

    #file metadata
    metadata = subp.add_parser('metadata')
    metadata_ops = metadata.add_subparsers()

    subcmd = metadata_ops.add_parser('list')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.add_argument('file_name', type=str)
    subcmd.set_defaults(func=file_metadata_list)

    subcmd = metadata_ops.add_parser('add')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.add_argument('file_name', type=str)
    subcmd.add_argument('meta_strs', type=str, nargs='*')
    subcmd.set_defaults(func=file_metadata_add)

    subcmd = metadata_ops.add_parser('delete')
    subcmd.add_argument('dataset_name', type=str)
    subcmd.add_argument('file_name', type=str)
    subcmd.set_defaults(func=file_metadata_delete)


def args_operation(args, clowder):

    try:
        args.func(args, clowder)
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        if status==401:
            sys.stderr.write('Operation not authorized. Your username' +
                              ' or password might be wrong or you have' +
                              ' no right on the file.\n')


def main():

    parser = argparse.ArgumentParser()
    setup_commands(parser) 
    args = parser.parse_args()

    clowder = Clowder(url=args.url, auth=(args.login, args.password))
    args_operation(args, clowder)

        
if __name__ == '__main__':
    main()
