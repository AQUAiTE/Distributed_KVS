import hashlib
import bisect

def sha256_hasher(key, shard_limit):
    """
    Contains the implementation of a sha256 hash algorithm converted to an int divided by the max number of shards.
    """
    encoded_key = key.encode()
    key_hex = hashlib.sha256(encoded_key).hexdigest()
    return int(key_hex, 16) % shard_limit

class ConsistentRing:
    def __init__(self, virtual_shards):
        """
        Initializes the ring for a consistent hashing protocol.

        :param virtual_shards: The number of virtual shards per shard
        """
        self.shard_locations = []               # Store the locations of shards on the ring
        self.shard_names = []                   # Store all shard names, including virtual shards 
        self.hash_limit = 2**16
        self.virtual_shards = virtual_shards
    
    def add_new_shard(self, shard):
        """
        Adds a real shard to the ring and creates virtual shards associated with it.
        This helps our key-value store have balanced loads.

        :param shard: The shard to add to the ring
        """
        # Hash it and use bisect to get the correct location around the ring
        shard_value = sha256_hasher(shard, self.hash_limit)
        ring_location = bisect.bisect(self.shard_locations, shard_value)
        self.shard_locations.insert(ring_location, shard_value)
        self.shard_names.insert(ring_location, shard)

        # Create virtual shards associated with the real shard
        for i in range(self.virtual_shards):
            virtual_shard = sha256_hasher(f"{shard}-{i}", self.hash_limit)
            bisect.insort(self.shard_locations, virtual_shard)
            insert_location = bisect.bisect_left(self.shard_locations, virtual_shard)
            # Insert the virtual shard based on bisect insert location
            self.shard_names.insert(insert_location, shard)
    
    def remove_shard(self, shard):
        """
        Removes a shard from the ring and removes all virtual shards

        :param shard: The shard to remove from the ring
        """
        # Calculate and determine where the given shard is located on the ring
        shard_value = sha256_hasher(shard, self.hash_limit)
        ring_location = bisect(self.shard_locations, shard_value)
        if self.shard_locations[ring_location] != shard_value:
            raise Exception("Shard isn't in the ring...\n")
        
        # Enumerate and reverse the locations list so we avoid out of bounds accesses
        shard_locations_enumerated = range(len(self.shard_locations))
        for location in reversed(shard_locations_enumerated):
            # Pop from our ring if we find the shard
            if self.shard_names[location] == shard:
                self.shard_names.pop(location)
                self.shard_locations.pop(location)
    
    def reset_ring(self):
        """
        Clears the ring of all shards
        """
        if self.shard_locations and self.shard_names:
            self.shard_locations.clear()
            self.shard_names.clear()
    
    # Description: Performs the "walk" in a consistent hashing assignment
    # USAGE: This method is used to figure out the hash value of a key and assign it to a shard
    # RETURN: Returns the shard name the key gets assigned to along with the hash value
    def key_to_shard(self, key):
        """
        Determines and assigns a key to a shard in the ring

        :param key: The key we want to find the shard assignment for
        RETURN: The shard assignment and the calculated hash value
        """
        hash_value = sha256_hasher(key, self.hash_limit)
        ring_location = bisect.bisect(self.shard_locations, hash_value) % len(self.shard_locations)
        return (self.shard_names[ring_location], hash_value)