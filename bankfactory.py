from Bank import Bank,Customer,Staff
import sys
import json

class BankFactory():
    @staticmethod
    def details(detail,name, pin, address,**kwargs):
        if detail.lower() == 'staff':
            return Staff(name, pin, address,**kwargs)
        if detail.lower()=='customer':
            return Customer(name, pin, address,**kwargs)
        
def main():
    detail = sys.argv[1]
    name = sys.argv[2]
    pin = sys.argv[3]
    address = sys.argv[4]
    kwargs = sys.argv[5:]

    args = {}
    for kwarg in kwargs:
        key,value = kwarg.split("=")
        args[key]= value

    bank_detail = BankFactory.details(detail,name,pin,address,**args)
    print(bank_detail.name)

    receiver = Staff('Namu',123,'Kalanki', 100)
    bank_detail.deposite(1000)
    bank_detail.withdrawal(500)
    bank_detail.transfer(100, receiver)
    bank_detail.pin = 4567
    print("New pin", bank_detail.get_pin) 
    print(bank_detail.status)

    data = {
        bank_detail.name:
        {'id': bank_detail.id,
        'name': bank_detail.name,
        'pin': bank_detail._Bank__pin, 
        'address': bank_detail.address,
        'balance': bank_detail.balance,
        'status': bank_detail.status}
    }
    with open(f'{detail}.json', 'w') as f:
        json.dump(data, f, indent=4)

if __name__ == '__main__':
    main()