import json

def decode_line(line):
    """Decode all complete objects; reject any trailing garbage or partial object."""
    decoder=json.JSONDecoder(); position=0; records=[]
    while position<len(line):
        while position<len(line) and line[position].isspace(): position+=1
        if position==len(line): break
        record,end=decoder.raw_decode(line,position)
        if not isinstance(record,dict): raise ValueError('Registro no es objeto')
        records.append(record); position=end
    if not records: raise ValueError('Línea vacía')
    return records
