import random

def getHeaderLine(fname):
    with open(fname) as f:
        headers = f.readline()
        return headers
    
def filesample(fname, desired_num_results, total_lines):
    result = []
    with open(fname) as f:
        #Save the header in any event
        result.append(f.readline())

        chances_selected = desired_num_results / total_lines
        for line in f:
            if random.random() < chances_selected:
                result.append(line)
        
    return result

def fileLen(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


