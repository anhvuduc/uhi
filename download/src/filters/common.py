import ee

def bitwise_extract(input_value, from_bit, to_bit):
    mask_size = 1 + to_bit - from_bit
    mask = ee.Number(1).leftShift(mask_size).subtract(1)
    return input_value.rightShift(from_bit).bitwiseAnd(mask)