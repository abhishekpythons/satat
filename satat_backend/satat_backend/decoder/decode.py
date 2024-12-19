import pandas as pd
import numpy as np

valid_lengths = [136, 74, 104, 65, 52, 126]
valid_apids = [1, 2, 3, 4, 5, 6]
packet_names = {1:'hk_pkt', 2:'Gmc', 3:'Comms', 4:'thermistor_pkt', 5:'init', 6:'log'}

   
# definitions
def fletcher(packet):
    '''function to check whether the calculated fletcher of the packet is equal to the written fletcher or not'''
    sumA = sumB = temp = 0
    for i in range(packet.index[0],packet.index[0]+len(packet)-2):
        sumA = ((sumA + packet[i])%255)
        sumB = ((sumB+sumA)%255)
        temp = 255-((sumA+sumB)%255)
        sumB = 255-((sumA+temp)%255)
        checksum = ((sumB << 8) | temp)
        #original_checksum = packet[packet.index[0]+len(packet)-1] << 8 | packet[packet.index[0]+len(packet)-2]
    return checksum

def show_packet(data_df, report_df, index):
    '''fetch the packet on the given index'''
    packet = data_df[int(report_df['packet_start'][index]):int(report_df['packet_start'][index]+report_df['length'][index])]
    print("packet in decimal is :-", packet)
    print("bytestream of packet is :- ", packet.values)

def decode_header(packet):
    header_fields = {'CCSDSVER': 3, 'CCSDSTYPE': 1, 'CCSDSSHF': 1, 'CCSDSAPID': 11, 'CCSDSSEQFLAGS': 2, 'CCSDSSEQCNT': 14, 'CCSDSLENGTH': 16, 'SHCOARSE': 32, 'SHFINE': 32}
    decoded_header_fields = {}
    binary_packet = ''.join(format(byte, '08b') for byte in packet)
    current_bit_index = 0

    for field, bit_count in header_fields.items():
        field_bits = binary_packet[current_bit_index:current_bit_index + bit_count]
        
        if field in ('SHCOARSE', 'SHFINE'):
            field_bits = ''.join(reversed([field_bits[i:i + 8] for i in range(0, len(field_bits), 8)]))

        decoded_header_fields[field] = int(field_bits, 2)
        current_bit_index += bit_count

    return decoded_header_fields
    

def decode_packet_data(packet, fields):
    decoded_data_fields = {}
    packet_index = 0

    for field in fields.keys():
        field_info = fields[field]

        if isinstance(field_info, str):
            # Process array fields (byte- or bit-based)
            parts = field_info.split('.')
            
            if len(parts) == 2:
                array_size, bytes_per_entry = map(int, parts)
                bits_per_entry = None
            elif len(parts) == 3:
                array_size, bytes_per_entry, bits_per_entry = map(int, parts)

            array_values = []

            if bits_per_entry is None:
                # Byte-based array
                for _ in range(array_size):
                    field_bytes = packet[packet_index:packet_index + bytes_per_entry]

                    if isinstance(field_bytes, pd.Series):
                        field_bytes = field_bytes.tolist()

                    if not field_bytes:
                        raise ValueError(f"Insufficient data for field '{field}' at index {packet_index}")

                    bin_value = ''.join(format(byte, '08b') for byte in reversed(field_bytes))
                    array_values.append(int(bin_value, 2))
                    packet_index += bytes_per_entry

            else:
                # Bit-based array
                total_bits = array_size * bits_per_entry
                total_bytes = (total_bits + 7) // 8
                field_bytes = packet[packet_index:packet_index + total_bytes]

                if isinstance(field_bytes, pd.Series):
                    field_bytes = field_bytes.tolist()

                if not field_bytes:
                    raise ValueError(f"Insufficient data for field '{field}' at index {packet_index}")

                bitstring = ''.join(format(byte, '08b') for byte in field_bytes)
                packet_index += total_bytes

                for i in range(array_size):
                    start = i * bits_per_entry
                    end = start + bits_per_entry
                    array_values.append(int(bitstring[start:end], 2))

            decoded_data_fields[field] = array_values

        else:
            # Process regular fields
            byte_count = field_info
            field_bytes = packet[packet_index:packet_index + byte_count]

            if isinstance(field_bytes, pd.Series):
                field_bytes = field_bytes.tolist()

            if not field_bytes:
                raise ValueError(f"Insufficient data for field '{field}' at index {packet_index}")

            bin_value = ''.join(format(byte, '08b') for byte in reversed(field_bytes))
            decoded_data_fields[field] = int(bin_value, 2)
            packet_index += byte_count

    return decoded_data_fields

def acc_conv(value, offset):
    conversion_factor = 0.0005978 
    
    if value & (1 << 15):  
        value -= 1 << 16 
        value = -value  
        value = value * conversion_factor  
        value -= offset  
        if value < 0:
            value = -value  
    else:
        value = -value  
        value = value * conversion_factor 
        value -= offset 
    
    return value

def ang_conv(value, axis):
    if axis == 'ROLL_AXIS':
        coeffs = [-0.9773453, 1.1570969, -2.934648e-03, 1.76749264e-04]
    elif axis == 'PITCH_AXIS':
        coeffs = [-1.6181640, 1.17534129, -2.6814129e-03, 1.0497870e-04]
    else:
        coeffs = [-3.1781066, 1.1893919, -4.1945017e-03, 1.712374e-04]

    if value & (1 << 15) != 0:
        value -= 1 << 16
        value = float(value)
        value *= 0.0152592547

        result = coeffs[0]
        raised_to_power = 1.0
        for coeff in coeffs[1:]:
            raised_to_power *= value
            result += (coeff * raised_to_power)

        if result < 0:
            result = -result

        return result

    value = float(value)
    value *= 0.0152592547

    result = coeffs[0]
    raised_to_power = 1.0
    for coeff in coeffs[1:]:
        raised_to_power *= value
        result += (coeff * raised_to_power)

    if result < 0:
        result = -result

    return result

def prc(x):
    values = [135, -0.3739, 6.964e-4, -7.588e-7, 4.7019e-10, -1.64087e-13, 2.997e-17, -2.227e-21]
    res = values[0]
    for i in range(1, len(values)):
        res += values[i] * pow(x, i)
    return res

# a string key value signifies that hte the output is an array with the format [size].[bytes per field]
# for fields with multiple decimal points, the format is [size].[buffer].[bits per field]
def decode_packets(packet, packet_type):
    init_fields = {'Image_ID': 1, 'status': '8.0.1', 'ADF_Init': 1, 'config': '8.0.1', 'GPS_Time_State_Vector': '32.1', 'Fletcher_Code': 2}
    gmc_fields = {'Image_ID': 1, 'GMC_Radiation_Counts': 4, 'GMC_Read_Free_Register': 4, 'GMC_Payload_Supply_Voltage': 2,'GM_Tube_High_Voltage': 2,'HVDC_IC_Control_Voltage': 2, 'Comparator_Reference_Voltage': 2,'Other_Channel_Of_ADC': '4.2','GMC_sd_dump': 1, 'GPS_Time_State_Vector': '32.1', 'Fletcher_Code': 2}
    therm_fields = {'GMC_Temperature': 2, 'PIS_Temperature': 2,'CUB_Temperature': 2,'OBC_Temperature': 2,'Sun_Facing_Connector_Temp': 2,'Sun_Facing_Flat_Temp': 2,'Adjacent_Sun_Facing_Window_Temp': 2,'Base_Plate_Temp': 2, 'sd_dump': 1, 'GPS_Time_State_Vector': '32.1', 'Fletcher_Code': 2}
    comms_fields = {'Image_ID': 1, 'Comms_ADF_CMD_Rx': 1, 'Comms_ADF_CMD_Succ': 1, 'Comms_ADF_CMD_REJECT': 1, 'Comms_ADF_RSSI_CCA': 2, 'Comms_ADF_RSSI': 2, 'Comms_ADF_Preamble_Pattern': 1, 'Comms_ADF_Sync_Word': 4, 'Comms_ADF_Freq': 4, 'Comms_ADF_Read_REG_ADDR': 4, 'Comms_ADF_Read_REG_No_Double_Words': 1, 'Comms_ADF_Data': '8.4', 'Comms_ADF_State': 1, 'sd_dump': 1, 'GPS_Time_State_Vector': '32.1', 'Fletcher_Code': 2}
    hk_fields = {'Cmd_ADF_Counts': 1, 'Cmd_RS485_Succ_Counts': 1, 'Cmd_RS485_Fail_Counts': 1, 'Image_ID': 1, 'CLK_Rate': 2, 'Command_Loss_Timer': 4, 'Prev_CMD_Receive': 1, 'Latest_CodeWord_RCV': 1, 'Reset_Counts': 1, 'RTM': '16.1', 'Acc_X': 2,'Acc_Y': 2,'Acc_Z': 2, 'Roll_Rate': 2,'Pitch_Rate': 2,'Yaw_Rate': 2, 'IMU_Temp': 2, 'CDH_Voltage': 2,'PIS_Voltage': 2,'Other_Voltages': '3.2', 'CDH_Current': 2,'PIS_Current': 2,'Other_Currents': '3.2', 'HK_Read_Pointer':4, 'HK_Write_Pointer': 4, 'Thermistor_Read_Pointer': 4, 'Thermistor_Write_Pointer': 4, 'Comms_Read_Pointer': 4, 'Comms_Write_Pointer': 4, 'sd_dump': 1, 'GPS_Time_State_Vector': '32.1', 'Fletcher_Code': 2}
    log_fields = {'TIMEL_1': 4, 'TIMEH_1': 4, 'TASKID_1': 1, 'TASK_STATUS_1': 2, 'TIMEL_2': 4, 'TIMEH_2': 4, 'TASKID_2': 1, 'TASK_STATUS_2': 2, 'TIMEL_3': 4, 'TIMEH_3': 4, 'TASKID_3': 1, 'TASK_STATUTS_3': 2, 'TIMEL_4': 4, 'TIMEH_4': 4, 'TASKID_4': 1, 'TASK_STATUS_4': 2, 'TIMEL_5': 4, 'TIMEH_5': 4, 'TASKID_5': 1, 'TASK_STATUS_5': 2, 'TIMEL_6': 4, 'TIMEH_6': 4, 'TASKID_6': 1, 'TASK_STATUS_6': 2, 'TIMEL_7': 4, 'TIMEH_7':4, 'TASKID_7': 1, 'TASK_STATUS_7': 2, 'TIMEL_8': 4, 'TIMEH_8': 4, 'TASKID_8': 1, 'TASK_STATUS_8': 2, 'TIMEL_9': 4, 'TIMEH_9': 4, 'TASKID_9': 1, 'TASK_STATUS_9': 2, 'TIMEL_10': 4, 'TIMEH_10': 4, 'TASKID_10': 1, 'TASK_STATUS_10': 2, 'Fletcher_Code': 2}
    decoded_header_fields = decode_header(packet[:14])
    
    if packet_type == 'init':
        decoded_data_fields = decode_packet_data(packet[14:], init_fields)
    elif packet_type == 'hk_pkt':
        decoded_data_fields = decode_packet_data(packet[14:], hk_fields)
        decoded_data_fields['CLK_Rate'] = decoded_data_fields['CLK_Rate'] * 0.001
        decoded_data_fields['Acc_X'] = acc_conv(decoded_data_fields['Acc_X'], 0.1680255)
        decoded_data_fields['Acc_Y'] = acc_conv(decoded_data_fields['Acc_Y'], 0.167197)
        decoded_data_fields['Acc_Z'] = acc_conv(decoded_data_fields['Acc_Z'], 0.1749625)
        decoded_data_fields['Roll_Rate'] = ang_conv(decoded_data_fields['Roll_Rate'], 'ROLL_AXIS')
        decoded_data_fields['Pitch_Rate'] = ang_conv(decoded_data_fields['Pitch_Rate'], 'PITCH_AXIS')
        decoded_data_fields['Yaw_Rate'] = ang_conv(decoded_data_fields['Yaw_Rate'], 'YAW_AXIS')
        decoded_data_fields['IMU_Temp'] = decoded_data_fields['IMU_Temp'] * 0.0625 + 25
        decoded_data_fields['CDH_Voltage'] = decoded_data_fields['CDH_Voltage'] * 0.001
        decoded_data_fields['PIS_Voltage'] = decoded_data_fields['PIS_Voltage'] * 0.001
        decoded_data_fields['CDH_Current'] = decoded_data_fields['CDH_Current'] * 0.5
        decoded_data_fields['PIS_Current'] = decoded_data_fields['PIS_Current'] * 0.5
    elif packet_type == 'Gmc':
        decoded_data_fields = decode_packet_data(packet[14:], gmc_fields)
        decoded_data_fields['GMC_Payload_Supply_Voltage'] = decoded_data_fields['GMC_Payload_Supply_Voltage'] * 0.00455
        decoded_data_fields['GM_Tube_High_Voltage'] = decoded_data_fields['GM_Tube_High_Voltage'] * 0.1519
        decoded_data_fields['HVDC_IC_Control_Voltage'] = decoded_data_fields['HVDC_IC_Control_Voltage'] * 0.004
        decoded_data_fields['Comparator_Reference_Voltage'] = decoded_data_fields['Comparator_Reference_Voltage'] * 0.0008
    elif packet_type == 'Comms':
        decoded_data_fields = decode_packet_data(packet[14:], comms_fields)
        decoded_data_fields['Comms_ADF_RSSI_CCA'] = decoded_data_fields['Comms_ADF_RSSI_CCA'] * -1
        decoded_data_fields['Comms_ADF_RSSI'] = decoded_data_fields['Comms_ADF_RSSI'] * -1
    elif packet_type == 'thermistor_pkt':
        decoded_data_fields = decode_packet_data(packet[14:], therm_fields)
        decoded_data_fields['GMC_Temperature'] = prc(decoded_data_fields['GMC_Temperature'])
        decoded_data_fields['PIS_Temperature'] = prc(decoded_data_fields['PIS_Temperature'])
        decoded_data_fields['CUB_Temperature'] = prc(decoded_data_fields['CUB_Temperature'])
        decoded_data_fields['OBC_Temperature'] = prc(decoded_data_fields['OBC_Temperature'])
        decoded_data_fields['Sun_Facing_Connector_Temp'] = prc(decoded_data_fields['Sun_Facing_Connector_Temp'])
        decoded_data_fields['Sun_Facing_Flat_Temp'] = prc(decoded_data_fields['Sun_Facing_Flat_Temp'])
        decoded_data_fields['Adjacent_Sun_Facing_Window_Temp'] = prc(decoded_data_fields['Adjacent_Sun_Facing_Window_Temp'])
        decoded_data_fields['Base_Plate_Temp'] = prc(decoded_data_fields['Base_Plate_Temp'])
        # decoded_data_fields['GPS_Time_State_Vector'] = prc(decoded_data_fields['GPS_Time_State_Vector']) # check and change
    elif packet_type == 'log':
        decoded_data_fields = decode_packet_data(packet[14:], log_fields)
    else:
        return None

    decoded_fields = decoded_header_fields | decoded_data_fields
    return decoded_fields
            
def packetiser(data_df, report_df):
    iterator = report_df.index
    decoded_values = []
    for i in iterator:
        packet = data_df[int(report_df['packet_start'][i]):int(report_df['packet_start'][i]+report_df['length'][i])].astype('uint8')
        decoded_values.append(decode_packets(packet, report_df['packet_type'][i]))

    return decoded_values

def summarize_data(data_df):
    index0x08 = data_df.where(data_df==0x08).dropna().index
    #print(index0x08)
    index0x08 = index0x08[:-1] #to elimate last packet which maybe incompleted
    valid_lengths_indexes = index0x08.where(data_df[index0x08+5].isin(valid_lengths)).dropna()
    valid_apids_indexes = index0x08.where(data_df[index0x08+1].isin(valid_apids)).dropna()
    valid_indexes = valid_apids_indexes.intersection(valid_lengths_indexes)
    report_df = pd.DataFrame({'packet_start': valid_indexes.astype('int'), 'apid': data_df[valid_indexes+1].values,'length': data_df[valid_indexes+5].values,}, index=valid_indexes)
    report_df.insert(1, "packet_type", data_df[valid_indexes+1].map(packet_names.get).values) #inserting packet_type column on the second column
    fletcher_byte_1 = data_df[report_df['packet_start']+report_df['length']-2]
    fletcher_byte_2 = data_df[report_df['packet_start']+report_df['length']-1]
    report_df['original_fletcher'] = (
    fletcher_byte_2.values.astype('int32') * 256 + fletcher_byte_1.values.astype('int32')).astype('int') #inserting original_fletcher column on the end of the columns
    report_df['calculated_fletcher'] = report_df.apply(lambda row: fletcher(data_df[row['packet_start']:row['packet_start']+row['length']]), axis=1)
    report_df['is_fletcher_correct'] = report_df.original_fletcher == report_df.calculated_fletcher #checking flether here
    report_df = report_df.reset_index(drop=True) #resetting index of fianl_df to 0 to n
    # final data_df is ready to export to files now
    # now we have processed and computed data in report_df and all raw data in data_df
    # both are equally important a report_df only have indexes of packets and lengths of those packets meanwhile data_df have the real data of  the whole file in bytes. 
    return report_df
