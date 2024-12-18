from django.shortcuts import render
from django.http import JsonResponse
from .decode import *
from .models import *

def file_input(request):
    return render(request, 'input_file.html')

def get_packet_by_index(data_df, summary_df, index):
    packet = data_df[int(summary_df['packet_start'][index]):int(summary_df['packet_start'][index]+summary_df['length'][index])].astype('uint8')
    return decode_packets(packet, summary_df['packet_type'][index])

def ccsds_decoder(request):
    # decoded_value = main()

    file = request.FILES['binary_input_file']
    data = file.read()
    byte_data = np.frombuffer(data, dtype=np.uint8)
    data_df = pd.Series(byte_data).iloc[::2].reset_index(drop=True)

    summary_df = summarize_data(data_df)

    # decoded_values = packetiser(data_df, summary_df)
    decoded_values_list = []  #Empty list to store decoded values

    for i in range(1,len(summary_df)):
        decoded_values = get_packet_by_index(data_df, summary_df, i)
    
        decoded_values_list.append(decoded_values)
    
    #Iterating over all packets
    for decoded_values in decoded_values_list:
        if decoded_values["CCSDSAPID"] == 1:
            new_hk_packet = HkPacket.objects.create(Filename=file.name, **decoded_values)
        elif decoded_values["CCSDSAPID"] == 2:
            new_gmc_packet = GmcPacket.objects.create(Filename=file.name, **decoded_values)
        elif decoded_values["CCSDSAPID"] == 3:
            new_comms_packet = CommsPacket.objects.create(Filename=file.name, **decoded_values)
        elif decoded_values["CCSDSAPID"] == 4:
            new_temp_packet = TempPacket.objects.create(Filename=file.name, **decoded_values)
        elif decoded_values["CCSDSAPID"] == 5:
            new_init_packet = InitPacket.objects.create(Filename=file.name, **decoded_values)
        else:
            pass

    return JsonResponse(decoded_values_list, safe=False)