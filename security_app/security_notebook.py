
        #This Library was developed by Christian Fuin as a mechanism to safely reuse usernames and passwords with windows, storing credentials
        #under the windows password management system and retriving them using a key which is hashed.


import keyring
import hashlib
import getpass



def three_md5_hash(key):
    """
        This function hashes a key using MD5 hash 3 times and returns a string with the key word hashed.
        Input: String
        Return: String (hashed)
    """
    for inter in range(0,3):
        key = hashlib.md5(key.encode()).hexdigest()
    return key   

def set_new_cred(key,usr):
    """
        This function sets a new pair of usr and password value under windows credentials manager. A hashed key is used to identify and retrieve the password
        Input: String (Key), String (usr)
    """
    print("Type your password to be store : ")
    try:
        keyring.set_password(three_md5_hash(key),usr,getpass.getpass())
        print("Password stored under Windows Credentials Manager successfully.\n Keep your key word handy as it will be used to retrieve your credentials...")
    except Exception as e:
        print(e)
        
def get_cred(key,usr):
    """
        This function retrieves the password value stored under windows credentials manager. A hashed key is used to identify and retrieve the password
        Input: String (Key), String (usr)
    """
    try:
        return keyring.get_password(three_md5_hash(key),usr)
    except Exception as e:
        print(e)

def main():   
   print("Setting new credentials")
   print("Type your user to be store : ")
   usr = getpass.getpass()
   print("Type your key to be store : ")
   key = getpass.getpass()

   set_new_cred(key,usr)


if __name__ == "__main__":
   print('""')
   main()




