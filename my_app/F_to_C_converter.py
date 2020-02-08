# This is a python code that asks a user to enter a temperature in
# Fahrenheit and returs the Celsius value


print("enter an integer to convert or anything else to break the loop")

while True:
	Fahrenheit = int(input("Enter a temperature in Fahrenheit: "))
	Celsius = (Fahrenheit - 32)*5.0/9.0
	print ("Temperature:", Fahrenheit, "F = ",Celsius,"C")

#	if Fahrenheit == 'Q' :
#		break


