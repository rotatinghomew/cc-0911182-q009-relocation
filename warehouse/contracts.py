from .checks import text


def normalize(table,original):
    row=dict(original)
    for field,value in row.items():
        if field=='year':
            if type(value) is not int or not 1<=value<=9999:
                raise ValueError('年份无效')
        else:
            text(value)
    return row
