import traceback
while True:
    temp = input()
    try:
        if temp.lower() == 'exit':
            #TODO: Confirm exit
            break
    except Exception as e:
        traceback.print_exc()
    