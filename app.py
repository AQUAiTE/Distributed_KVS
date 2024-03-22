from flask import Flask, request, jsonify
from collections import defaultdict
import requests, os
import time
import random
import jsonpickle
from consistent_hash import ConsistentRing

# Initializations
MY_ADDRESS = os.environ['SOCKET_ADDRESS']
MY_VIEW = os.environ['VIEW']
try:
    shard_count = int(os.environ['SHARD_COUNT'])
except KeyError:
    shard_count = None

shards = {}
shard_value_map = {} 
current_shard = None
Store = {}
VectorClock = {}
consistentRing = ConsistentRing(1000)
View = {}
View = set(MY_VIEW.split(',')) 
View.add(MY_ADDRESS)

app = Flask(__name__)

# Description: 
def blast_add(new_replica_socket_address):
    """
    Sends a request to each replica in this replica's view, asking to be placed in their View.
    :param new_replica_socket_address: Will always be the current replica's address
    """
    for rep in View:
        if rep == MY_ADDRESS:
            continue

        rep_url = f"http://{rep}/viewed"
        data = {"socket-address": new_replica_socket_address}
        
        try:
            res = requests.put(rep_url, json=data, timeout=1.5)
            if res.status_code == 200 or res.status_code == 201:
                print(f"A PUT to {rep} was successful")
        except requests.exceptions.Timeout:
            print(f"A PUT request to {rep} timed out")
        except requests.exceptions.RequestException as e:
            print(f"We ran into a non-timeout error when sending a PUT request to {rep}")

def init_shards(num_shards):
    """Initializes vars related to sharding after verifying that enough replicas exist."""
    global current_shard, shard_value_map, consistentRing
    shard_building = {}
    for i in range(num_shards):
        shard_building[f"s{i}"] = list()

    # Insert replicas into shards evenly
    for j, replica in enumerate(sorted(View)):
        shard_building[f"s{j % num_shards}"].append(replica)
    
    # Build shard->keys map and add shard to hash ring
    for shard, replicas in shard_building.items():
        if len(replicas) < 2:
            raise notEnoughShardsError
        if MY_ADDRESS in replicas:
            current_shard = shard
        shard_value_map[shard] = set()
        consistentRing.add_new_shard(shard)
    
    return shard_building

class notEnoughShardsError(Exception):
    pass

@app.route('/existinginfo', methods=['GET'])
def send_info():
    return {'store': Store, 'vc': VectorClock}, 200

def get_info():
    """Until it receives a response, asks every replica in its view for the VC and Store data."""
    if current_shard is not None:
        for rep in shards[current_shard]:
            if rep != MY_ADDRESS:
                rep_url = f"http://{rep}/existinginfo"
                try:
                    response = requests.get(rep_url, timeout=0.5)
                    if response.status_code == 200:
                        data = response.json()
                        global Store, VectorClock
                        Store = data.get('store')
                        VectorClock = data.get('vc')
                        return True
                except requests.exceptions.RequestException as e:
                    continue

# Initialize shards and VC
blast_add(MY_ADDRESS)
if shard_count is not None:
    shards = init_shards(shard_count)
    for shard, replicas in shards.items():
        if MY_ADDRESS in replicas:
            current_shard = shard

for i in View:
    VectorClock[i] = 0

get_info()



# Key-Value Store Operations =================================================================================
def LessThanOrEqualTo(VC_Client, VC_Replica):
    """
    Performs Vector Clock comparisons.
    Returns True if the client's VC from their metadata is less than or equal to the replica's current VC.
    """
    if VC_Client == None:
        return True
    for client_key, client_value in VC_Client.items():
        if client_key not in VC_Replica:
            return False  # Client clock has a key that is not present in replica clock
        replica_value = VC_Replica[client_key]
        
        if client_value > replica_value:
            return False  # Client clock has a higher value for a key than replica clock
            
    return True

# Forwarding and Broadcast Operations -----------------------------------------------------------------
def forwardget(i, key, vc):
    """
    Forwards a GET request to the shard that contains the requested key.
    
    :param i: The shard that contains the key
    :param key: The key that the client wants the value of
    :param vc: The vector clock that the client sent to the original request 
    """
    shard = shards[i]
    repl = random.randrange(len(shard))
    data = {'causal-metadata': vc}
    # Forward the request
    try:
        rep_url = f"http://{shard[repl]}/kvs/{key}"
        res = requests.get(rep_url, json=data, timeout=2)
        return res
    except requests.exceptions.Timeout:
        print(f"A PUT request to {repl} timed out")
    except requests.exceptions.RequestException as e:
        print(f"We ran into a non-timeout error when sending a PUT request to {repl}")

def forwardput(i, key, value, vc):
    """
    Forwards a PUT request to the shard that should contain the key.
    
    :param i: The shard that contains the key
    :param key: The key that the client wants to insert into the key-value store
    :param value: The value that the client wants the key to have
    :param vc: The vector clock that the client sent to the original request 
    """
    shard = shards[i]
    rep = random.randrange(len(shard))
    rep_url = f"http://{shard[rep]}/kvs/{key}"
    data = {"value": value, "causal-metadata": vc}
    # Forward the request
    try:
        res = requests.put(rep_url, json=data, timeout=2)
        if res.status_code == 200 or res.status_code == 201:
            return res
    except requests.exceptions.Timeout:
        print(f"A PUT request to {rep} timed out")
    except requests.exceptions.RequestException as e:
        print(f"We ran into a non-timeout error when sending a PUT request to {rep}")

def forwarddelete(i, key):
    """
    Forwards a delete request to the shard that has key <key>.

    :param i: Specifies the shard that contains key <key>
    :param key: The key that is to be deleted
    """
    for rep in shards[i]:
        rep_url = f"http://{rep}/kvs/{key}"
        data = {"causal-metadata": None}

        try:
            res = requests.delete(rep_url, json=data, timeout=0.5)
            if res.status_code == 200:
                return res
        except requests.exceptions.Timeout:
            print(f"A PUT request to {rep} timed out")
        except requests.exceptions.RequestException as e:
            print(f"We ran into a non-timeout error when sending a PUT request to {rep}")

def blast_vc():
    """
    Broadcasts the vector clock of this replica to maintain causal consistency at all replicas.
    """
    for rep in View:
        if rep != MY_ADDRESS:
            rep_url = f"http://{rep}/reptorep/updatevc"
            data = {"vc": VectorClock}

            try:
                res = requests.put(rep_url, json=data, timeout=2.5)
                if res.status_code == 200:
                    print(f"Successful blast to {rep}")
            except requests.exceptions.Timeout:
                print(f"A PUT request to {rep} timed out")
            except requests.exceptions.RequestException as e:
                print(f"We ran into a non-timeout error when sending a PUT request to {rep}")

def blast_put_key(key, value, from_rep):
    """
    Repeats PUT request unicasts to the shard that <key> should be assigned to until a successful PUT.

    :param key: The key that the client wishes to insert to the store
    :param value: The value that the key should have
    :param from_rep: The replica that originally received the request
    """
    temp_View = View
    to_remove = set()
    # Locate the correct shard to forward the PUT request to
    for rep in shards[current_shard]:
        if rep != MY_ADDRESS:
            rep_url = f"http://{rep}/reptorep/{key}/{from_rep}"
            data = {"value": value, "causal-metadata": VectorClock}
            retries = 0
            while retries < 2:
                try:
                    response = requests.put(rep_url, json=data, timeout=0.9)
                    if response.status_code == 200 or response.status_code == 201:
                        return True
                except requests.exceptions.ConnectionError:
                    if rep in View:
                        to_remove.add(rep)
                retries += 1
                time.sleep(0.1)
                
    for rep in to_remove:
        View.remove(rep)
        blast_delete(rep)

def blast_delete_key(key, from_rep):
    """
    Broadcasts a DELETE request to the replicas in the shard that should contain <key>.

    :param key: The key that is to be deleted from the key-value store.
    :param from_rep: The replica that originally received the DELETE request.
    """
    temp_View = View
    to_remove = set()

    # Locate and attempt to forward the DELETE request to each replica for the shard containing key <key>
    for rep in shards[current_shard]:
        if rep != MY_ADDRESS:
            rep_url = f"http://{rep}/reptorep/{key}/{from_rep}"
            data = {"causal-metadata": VectorClock, "from_shard":current_shard}
            retries = 0
            while retries < 2:
                try:
                    response = requests.delete(rep_url, json=data, timeout=1.0)
                    if response.status_code == 200 or response.status_code == 201:
                        max_clock = {}
                        new_VC = response.json()["causal-metadata"]
                        for rep_vc in set(VectorClock.keys()).union(new_VC.keys()):
                            VectorClock[rep_vc] = max(VectorClock.get(rep_vc, 0), new_VC.get(rep_vc, 0))
                        break
                except requests.exceptions.ConnectionError:
                    if rep in View:
                        to_remove.add(rep)
                retries += 1
                time.sleep(0.5)
    for rep in to_remove:
        View.remove(rep)
        blast_delete(rep)

def blast_map(key):
    """
    Broadcast an update of the mapping of which shards holds the inserted key.

    :param key: The key that was inserted/updated in the key-value store
    """
    for rep in View:
        if rep != MY_ADDRESS:
            rep_url = f"http://{rep}/reptorep/updatemap/{key}"
            data = {"shard": current_shard}

            try:
                res = requests.put(rep_url, json=data, timeout=2.5)
                if res.status_code == 200:
                    print(f"Successful blast to {rep}")
            except requests.exceptions.Timeout:
                print(f"A PUT request to {rep} timed out")
            except requests.exceptions.RequestException as e:
                print(f"We ran into a non-timeout error when sending a PUT request to {rep}")

@app.route('/reptorep/<key>/<from_rep>', methods=['PUT'])
def Rec_Val_From_Rep(key, from_rep):
    """
    Receive and handle a forwarded PUT request.

    :param key: The key that we need to insert/update
    :param from_rep: The replica that originally received this request
    """
    data = request.json
    VC_Incoming = data.get('causal-metadata')

    # Check if Request has Val
    if 'value' not in data:
        return {"error": "PUT request does not specify a value"}, 400
    
    # Check for Length of Key
    if len(key) > 50:
        return {"error": "Key is too long"}, 400
    value = data.get('value')

    # Check if new key
    if key not in Store.keys():
        Store[key] = value
        blast_vc()
        shard_value_map[current_shard].add(key)
        return {"result": "created", "causal-metadata": VectorClock}, 201
    else:
        Store[key] = value
        blast_vc()
        return {"result": "replaced", "causal-metadata": VectorClock}, 200

@app.route('/reptorep/<key>/<from_rep>', methods=['DELETE'])
def Rec_Val_From_Rep_del(key, from_rep):
    """
    Handle a forwarded request to delete a key.

    :param key: The key that is to be deleted from the key-vakue store
    :param from_rep: The replica that originally received this request
    """
    data = request.json

    # Handle the VC
    VC_Incoming = data.get('causal-metadata')
    VectorClock[from_rep] = VectorClock[from_rep] + 1
    from_shard = data.get('from_shard')

    # Add the key to this replica's key-shard map
    shard_value_map[from_shard].add(key)
    
    #Check for Length of Key
    if len(key) > 50:
        return {"error": "Key is too long"}, 400

    # Make sure VCs are in sync
    for rep in set(VectorClock.keys()).union(VC_Incoming.keys()):
        VectorClock[rep] = max(VectorClock.get(rep, 0), VC_Incoming.get(rep, 0))

    check = LessThanOrEqualTo(VC_Incoming, VectorClock)
    if check == False:
        return {"error": "Causal dependencies not satisfied; try again later"}, 503

    for rep in set(VectorClock.keys()).union(VC_Incoming.keys()):
        VectorClock[rep] = max(VectorClock.get(rep, 0), VC_Incoming.get(rep, 0))

    #Check if new key
    if key not in Store.keys():
        blast_vc()
        return {"result": "created", "causal-metadata": VectorClock}, 201
    else:
        del Store[key]
        blast_vc()
        return {"result": "replaced", "causal-metadata": VectorClock}, 200

@app.route('/reptorep/updatevc', methods=['PUT'])
def updatevc():
    """
    Receive a Vector Clock update broadcast.
    """
    global VectorClock
    data = request.json
    vc = data.get('vc')
    VectorClock = vc
    return {"result": "sucessful update"}, 200

@app.route('/reptorep/updatemap/<key>', methods=['PUT'])
def updatemap(key):
    """
    Receive an update for the mapping of keys to shards.
    """
    data = request.json
    shard = data.get('shard')
    shard_value_map[shard].add(key)
    return {"result": "successful update"}, 200

# APIs used by clients to interact with KV-Store -----------------------------------------------------------------
@app.route('/kvs/<key>', methods=['GET'])
def Get_Val_at_Rep(key):    
    """
    Return to the client the value of <key> according to the store.

    :param key: The key to return the value of
    """
    global Store
    data = request.json
    VC_Client = data.get('causal-metadata')
    
    # Determine if this GET request is causally consistent
    check = LessThanOrEqualTo(VC_Client, VectorClock)

    # Causally Consistent Request
    if check == True:
        # We need to forward this request since we don't have this key
        if key not in Store.keys():
            for i in shard_value_map:
                if key in shard_value_map[i]:
                    res = forwardget(i, key, VC_Client)
                    dataforwarded = res.json()
                    vc = dataforwarded.get('causal-metadata')
                    k = dataforwarded.get('value')
                    return {"result": "found", "value": k, "causal-metadata": vc}, 200
                    
            return {"error": "Key does not exist"}, 404
        else:
            blast_vc()
            return {"result": "found", "value": Store[key], "causal-metadata": VectorClock}, 200
    # Non Causally Consistent Request
    else:
        return {"error": "Causal dependencies not satisfied; try again later"}, 503

@app.route('/kvs/<key>', methods=['PUT'])
def Put_Val_at_Rep(key):
    """
    Handles a PUT request by a client by inserting <key> into the proper shard.

    :param key: The key that is to be inserted
    """
    data = request.json
    value = data.get('value')
    vc = data.get('causal-metadata')

    # Use our consistent hashing ring to determine the appropriate shard
    shard, hash_value = consistentRing.key_to_shard(key)

    # Forward the request if our shard isn't assigned this key
    if shard != current_shard:
        res = forwardput(shard, key, value, vc)
        return jsonify(res.json()), res.status_code
    
    # This key goes into our shard
    else:
        # Verify causal consistency
        VC_Incoming = data.get('causal-metadata')
        check = False
        if VC_Incoming:
            check = LessThanOrEqualTo(VC_Incoming, VectorClock)

        # Null PUT metadata has no dependencies
        else:
            check = True

        if check == True:
                #Check if Request has Val
                if 'value' not in data:
                    return {"error": "PUT request does not specify a value"}, 400
                
                #Check for Length of Key
                if len(key) > 50:
                    return {"error": "Key is too long"}, 400
                
                #Increment VC of Replica and check causality again
                VectorClock[MY_ADDRESS] = VectorClock[MY_ADDRESS] + 1
                check = LessThanOrEqualTo(VC_Incoming, VectorClock)
                if check == False:
                    return {"error": "Causal dependencies not satisfied; try again later"}, 503
                
                # Broadcast the PUT to other replicas in my shard
                blast_vc()
                blast_put_key(key, value, MY_ADDRESS)
                if key not in Store.keys():
                    Store[key] = value
                    shard_value_map[current_shard].add(key)
                    blast_map(key)
                    return {"result": "created", "causal-metadata": VectorClock, "shard-id": current_shard}, 201
                else:
                    Store[key] = value
                    return {"result": "replaced", "causal-metadata": VectorClock}, 200

@app.route('/kvs/<key>', methods=['DELETE'])
def Delete_Val_at_Rep(key):
    """
    Handle a DELETE request by a client for key <key>.

    :param key: The key to delete from the key-value store
    """
    data = request.json

    # Verify causal consistency
    # Note that a 503 for DELETE requests is IMPOSSIBLE
    VC_Incoming = data.get('causal-metadata')
    check = False
    if VC_Incoming:
        # Check Causal History to See if Less-Than-Or-Equal-To
        print(f"VC_Incoming: {VC_Incoming}")
        check = LessThanOrEqualTo(VC_Incoming, VectorClock)

    # Null DELETE metadata has no dependencies
    else:
        print(f"VC_Incoming: Nothing yet!")
        check = True

    # Causally consistent request
    if check == True:
        # Check if Key Exists
        if key not in Store.keys():
            for i in shard_value_map:
                if key in shard_value_map[i]:
                    res = forwarddelete(i, key)
                    dataforwarded = res.get_json()
                    shard_value_map[current_shard].remove(key)
                    blast_vc()
                    return {"result": "deleted", "causal-metadata": VectorClock}, 200
            return {"error": "Key not found"}, 404
        
        del Store[key]
        blast_delete_key(key, MY_ADDRESS)

        # Increment VC of Replica
        VectorClock[MY_ADDRESS] += 1
        blast_vc()
        return {"result": "deleted", "causal-metadata": VectorClock}, 200

    # Else Return Causal Not Satisfied
    else:
        return {"error": "Causal dependencies not satisfied; try again later"}, 503

# View Operations ===========================================================================
@app.route('/view', methods=['PUT'])
def create_new_replica():
    """
    Add a new replica to the View and VC of all existing replicas.
    """
    data = request.json
    new_replica_socket_address = data.get('socket-address')

    # Already Exists in View
    if new_replica_socket_address in View:
        return {"result": "already present"}, 200

    View.add(new_replica_socket_address);
    VectorClock[new_replica_socket_address] = 0

    # Broadcast new replica addition to all other replicas
    blast_add(new_replica_socket_address)
    return {"result": "added"}, 201

@app.route('/viewed', methods=['PUT'])
def create_new_replica_from_blast():
    """
    Non-initializing replica adds the new replica to its View and VC.
    """
    data = request.json
    new_replica_socket_address = data.get('socket-address')

    # Already Exists in View
    if new_replica_socket_address in View:
        return {"result": "already present"}, 200

    View.add(new_replica_socket_address);
    VectorClock[new_replica_socket_address] = 0
    return {"result": "added"}, 201

@app.route('/view', methods=['GET'])
def get_view():
    """
    Returns all replicas that requested replica is aware of.
    """
    return {"view": list(View)}, 200


# Delete Replica Operations ====================================================
@app.route('/view', methods=['DELETE'])
def delete_replica():
    """
    Deletes a replica from the network of replicas.
    """
    data = request.json
    replica_socket_address = data.get('socket-address')

    # Doesn't Exists in View
    if replica_socket_address not in View:
        return {"error": "View has no such replica"}, 404
    
    View.remove(replica_socket_address)

    # Broadcast the delete
    blast_delete(replica_socket_address)
    return {"result": "deleted"}, 200


def blast_delete(replica_socket_address):
    """
    Broadcasts the delete of the replica given at <replica_socket_address>
    """
    for rep in View:
        if rep != MY_ADDRESS:
            rep_url = f"http://{rep}/viewed"
            data = {"socket-address": replica_socket_address}
            
            try:
                res = requests.delete(rep_url, json=data, timeout=1)

                # Testing lines only
                if res.status_code == 200 or res.status_code == 404:
                    print(f"A DELETE to {rep} was successful")

            except requests.exceptions.Timeout:
                print(f"A DELETE request to {rep} timed out")
            except requests.exceptions.RequestException as e:
                print(f"We ran into a non-timeout error when sending a DELETE request to {rep}")

@app.route('/viewed', methods=['DELETE'])
def delete_replica_from_blast():
    """
    Deletes a replica from this replica's view.
    """
    data = request.json
    replica_socket_address = data.get('socket-address')

    #Doesn't Exists in View
    if replica_socket_address not in View:
        return {"error": "View has no such replica"}, 404

    View.remove(replica_socket_address)
    return {"result": "deleted"}, 200


# Shard Operations =====================================================================================
@app.route('/shard/ids', methods=['GET'])
def get_shard_id():
    """
    Returns the list of shard ids.
    """
    return {"shard-ids": list(shards.keys())}, 200

@app.route('/shard/node-shard-id', methods=['GET'])
def get_node_share_id():
    """
    Returns the shard id of the receiving replica.
    """
    return {'node-shard-id': current_shard}, 200

@app.route('/shard/members/<id>', methods=['GET'])
def shard_members(id):
    """
    Returns the list of members in shard <id>.
    """
    if id in shards.keys():
        return {'shard-members': shards[id]}, 200
    else:
        return {"error": "Shard does not exist"}, 404

@app.route('/shard/key-count/<id>', methods=['GET'])
def shard_key_count(id):
    """
    Returns the number of keys in shard <id>.
    """
    if id in shards.keys():
        return {'shard-key-count': len(shard_value_map[id])}, 200
    else:
        return {"error": "id not in shard keys"}, 404
    
@app.route('/shard/add-member/<id>', methods=['PUT'])
def shard_add_member(id):
    """
    Adds a new member to the shard specified by <id> and syncs its data with all members of shard <id>.
    """
    data = request.json
    node_port = data.get('socket-address')

    if node_port in View and id in shards.keys():
        blast_add_member(id, node_port)
        return {"result": "node added to shard"}, 200
    else:
        return {"error": "Either id or Node:Port doesn't exist"}, 404

def blast_add_member(id, node_port):
    """
    Broadcasts the addition of a new member to all replicas in the initial replica's shard.
    """
    global current_shard, Store, VectorClock, shards, shard_value_map, consistentRing
    for rep in View:
        rep_url = f"http://{rep}/shard/addmemberincoming"
        
        # Convert to json serializable format
        mapping_as_list = {shard: list(value_set) for shard, value_set in shard_value_map.items()}
        ring_encode = jsonpickle.encode(consistentRing)
        data  = {"id": id, "node_port": node_port, "store": Store, "vc": VectorClock, "shards": shards, "mapping": mapping_as_list, "ring": ring_encode}
        try:
            res = requests.put(rep_url, json=data, timeout=0.7)
            if res.status_code == 201:
                print("Success")
        except requests.exceptions.Timeout:
            print(f"A PUT request to {rep} timed out")
        except requests.exceptions.RequestException as e:
            print(f"We ran into a non-timeout error when sending a PUT request to {rep}")

@app.route('/shard/addmemberincoming', methods=['PUT'])
def add_member_incoming():
    """
    If not the new member: Adds member to my list of replicas in each shard.
    If new member: Initialize my values to be in sync with replicas in my assgined shard.
    """
    global current_shard
    data = request.json
    node_port = data.get('node_port')
    id = data.get('id')
    store = data.get('store')
    vc = data.get('VectorClock')
    shard_set = data.get('shards')
    mapping = data.get('mapping')
    ring = data.get('ring')

    if node_port == MY_ADDRESS:
        global Store, VectorClock, shards, shard_value_map, consistentRing
        current_shard = id
        Store = store
        VectorClock = vc
        shards = shard_set
        shard_value_map = {shard: set(value_list) for shard, value_list in mapping.items()}
        consistentRing = jsonpickle.decode(ring)
    
    shards[id].append(node_port)

    return {"result": "incoming done"}, 201

@app.route('/shard/reshard', methods=['PUT'])
def reshard():
    """
    Performs a reshard operation that is initiated by the replica receiving the client request.
    Process:
    - Determines the new shard assignments for all replicas and broadcasts the result.
    - Performs a rehash of all its key-value pairs and tells all replicas to do the same.
    - Sends out rehash results and tells all replicas to do the same.
    """
    global Store, shards, shard_value_map, current_shard, consistentRing
    data = request.json
    new_shard_count = data.get('shard-count')
        
    # Verify that we do not violate fault-tolerance with new shard count
    if new_shard_count * 2 > len(View):
        return {"error": "Not enough nodes to provide fault tolerance with requested shard count"}, 400
    
    # Reset and rebuild the shards (also remakes the shard ring)
    consistentRing.reset_ring()
    new_shards = init_shards(new_shard_count)
    
    # Reshard broadcast
    shards = new_shards
    ring = jsonpickle.encode(consistentRing)
    for rep in View:
        if rep != MY_ADDRESS:
            rep_url = f"http://{rep}/shard/blast_reshard"
            data = {"shards": new_shards, "ring": ring}
            try:
                res = requests.put(rep_url, json=data, timeout=1.5)
                if res.status_code == 200:
                    print("Success")
            except requests.exceptions.Timeout:
                print(f"A RESHARD request to {rep} timed out")
            except requests.exceptions.RequestException as e:
                print(f"We ran into a non-timeout error when sending a RESHARD request to {rep}")

    # Now remap all the kv-pairs and prepare to receive new kv-pairs
    if Store != {}:
        my_remapping = rehash(Store)
        Store = {}
        
        for shard, pairs in my_remapping.items():
            for rep in View:
                if rep != MY_ADDRESS:
                    rep_url = f"http://{rep}/reptorep/remap"
                    try:
                        res = requests.put(rep_url, timeout=4)
                        if res.status_code == 200:
                            print("Success")
                    except requests.exceptions.Timeout:
                        print(f"A UPDATE STORE request to {rep} timed out")
                    except requests.exceptions.RequestException as e:
                        print(f"We ran into a non-timeout error when sending a UPDATE STORE request to {rep}")
            for rep2 in shards[shard]:
                rep_url = f"http://{rep2}/reptorep/updated_store"
                data = {'new-store': pairs}
                try:
                    res = requests.put(rep_url, json=data, timeout=1.5)
                    if res.status_code == 200:
                        print("Success")
                except requests.exceptions.Timeout:
                    print(f"A UPDATE STORE request to {rep} timed out")
                except requests.exceptions.RequestException as e:
                    print(f"We ran into a non-timeout error when sending a UPDATE STORE request to {rep}")                    

    return {"result": "resharded"}, 200


# Reshard APIs and Functions ===============================================================   
@app.route('/shard/blast_reshard', methods=['PUT'])
def blasted_reshard():
    """
    Updates the shard members for all replicas that did not initiate the reshard.
    """
    global Store, shards, shard_value_map, current_shard, consistentRing
    data = request.json
    shards = data.get('shards')
    for shard, reps in shards.items():
        if MY_ADDRESS in reps:
            current_shard = shard
        shard_value_map[shard] = set()
    consistentRing = jsonpickle.decode(data.get('ring'))

    return {"result": "resharded"}, 200

@app.route('/reptorep/remap', methods=['PUT'])
def update_store():
    """
    Rehashes the key-value store, then sends the pairs to their new correct shard location.
    """
    global Store
    if Store != {}:
        my_remapping = rehash(Store)
        Store = {}
        time.sleep(1)

        for shard, pairs in my_remapping.items():
            shard_to_send = shards[shard]
            for rep in shard_to_send:
                rep_url = f"http://{rep}/reptorep/updated_store"
                data = {'new-store': pairs}
                try:
                    res = requests.put(rep_url, json=data, timeout=1)
                    if res.status_code == 200:
                        print("Success")
                except requests.exceptions.Timeout:
                    print(f"A UPDATE STORE request to {rep} timed out")
                except requests.exceptions.RequestException as e:
                    print(f"We ran into a non-timeout error when sending a UPDATE STORE request to {rep}")
    return {"result": "successful remap"}, 200

@app.route('/reptorep/updated_store', methods=['PUT'])
def updated_store():
    """
    Receives its shard's assigned key-value pairs.
    """
    global Store
    data = request.json
    Store.update(data.get('new-store'))

    return {"result": "update successful"}, 200

@app.route('/reptorep/updated_map', methods=['PUT'])
def updated_map():
    """
    Updates its map of what shard's hold which keys.
    """
    global shard_value_map
    data = request.json
    new_map = jsonpickle.decode(data.get('new-map'))

    # Merge the new data with the existing values
    for shard, keys in new_map.items():
        if keys != set():
            shard_value_map[shard].update(keys)

    return {"result": "Update Succesful"}, 200

def rehash(store):
    """
    Uses consistent hashing to re-determine the shard location for each key.
    Creates and broadcasts to every replica a new mapping of keys to shards.
    Returns a defaultdict(dict) object with every shard as the key holding key-value pairs
    """
    global shards, shard_value_map
    # Use a defaultdict so every shard gets a dictionary of key value pairs
    new_mapping = defaultdict(dict)
        
    for key, value in store.items():
        new_shard, hash_value = consistentRing.key_to_shard(key)
        new_mapping[new_shard][key] = value
        shard_value_map[new_shard].add(key)

    for rep in View:
        if rep != MY_ADDRESS:
            rep_url = f"http://{rep}/reptorep/updated_map"
            data = {'new-map': jsonpickle.encode(shard_value_map)}
            try:
                res = requests.put(rep_url, json=data, timeout=1)
                if res.status_code == 200:
                    print("Successful Mapping Update")
            except requests.exceptions.Timeout:
                print(f"A UPDATE MAP request to {rep} timed out")
            except requests.exceptions.RequestException as e:
                print(f"We ran into a non-timeout error when sending a UPDATE MAP request to {rep}")

    return dict(new_mapping)



#Main =====================================================================
if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=8090, threaded=True)
