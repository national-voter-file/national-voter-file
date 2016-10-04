# Create string translation tables
allowed = ' _01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
delchars = ""
for i in range(255):
	if chr(i) not in allowed: delchars = delchars + str(chr(i))
deltable = str.maketrans(' ','_', delchars)

def fixColName(col):
    # Format column name to remove unwanted chars
    col = str.strip(col)
    col = col.translate(deltable)
    fmtcol = col
    fmtcol = col.lower()
	
    return fmtcol

    
