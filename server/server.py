#!/usr/bin/env python

from datetime import datetime
import websockets

from protobufs.letters_pb2 import Letters
import string
import time

import asyncio
import concurrent.futures
from threading import current_thread



# task of sending to the client
async def send(websocket, return_message_bytes, return_message_is_english):

	# if inputs are alphabets, wait for 10 sec
	# if not, just send back and trigger the alert
	if (return_message_is_english == 1):
		print("(", datetime.now(), ") sleep for 10 sec")
		print()
		await asyncio.sleep(10)	

	# send the message
	await websocket.send(return_message_bytes)
	print ("(", datetime.now(), ") send back message: ",return_message_bytes)
	print()

# construct the main job of receiving a message
def LetterCaseConverter(websocket, message):

	print("(", datetime.now(), ") working in thread: ", current_thread())

	# decode the message
	return_message = Letters()
	return_message.ParseFromString(message)
	print ("(", datetime.now(), ") decode message: ",return_message.input_letters,return_message.is_english)

	# check if inputs are alphabets
	# ignore spaces and punctuations when checking
	if (return_message.input_letters.replace(" ","").translate(str.maketrans('', '', string.punctuation)).isalpha()==True):
		# true: convert the message 
		return_message.is_english = 1
		return_message.input_letters = return_message.input_letters.swapcase()
	else:
		# false: enable the alert 
		return_message.is_english = 0
	print ("(", datetime.now(), ") converted message: ",return_message.input_letters,return_message.is_english)

	# encode the message
	return_message_bytes = return_message.SerializeToString()
	print ("(", datetime.now(), ") encoded converted message: ",return_message_bytes)
	print()

	# queue the task of sending back message
	#asyncio.ensure_future(send(websocket,return_message_bytes,return_message.is_english), loop=loop)
	asyncio.run_coroutine_threadsafe(send(websocket,return_message_bytes,return_message.is_english), loop)

# construct the task of a client connection
async def OneClientTask(websocket, path):

	# show the number of clients when new client is connected
	global client_num
	client_num += 1
	print("(", datetime.now(), ") established one connection to ", websocket.remote_address[0],",", client_num, "client connected")
	print()

	# construct a pool of threads
	executor = concurrent.futures.ThreadPoolExecutor() # defaulf max_workers=20

	try:

		# keep receiving message from the client
		async for message in websocket:

			# receive the message
			print("(", datetime.now(), ") received message: ",message)

			# construct a thread and run the main job in the thread
			executor.submit(LetterCaseConverter, websocket, message)

			#print("(", datetime.now(), ") number of threads: ", len(executor._threads))

	# listen to connection and show the number of clients when a client is disconnected	
	except websockets.exceptions.ConnectionClosed:

		# show the number of clients
		client_num -= 1
		print("(", datetime.now(), ") lost connection from ",websocket.remote_address[0],",", client_num, "client connected")
		print()



print( "(", datetime.now(), ") server started (press Ctrl-C to exit the server)" )

client_num = 0 # number of clients connected to the server

# create a event loop
loop = asyncio.get_event_loop()

# setup a task that connects to the server
start_server = websockets.serve(OneClientTask, "localhost", 5675)

# run the task
try:
	loop.run_until_complete(start_server)
	loop.run_forever()

# listen for ctrl c to terminate the program
except KeyboardInterrupt:
	loop.stop()
	print("\n(", datetime.now(), ") exiting the server")

