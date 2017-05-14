import json
import logging
from service.database_service import Database
from service.device_data_process_service import ProcessedCallData, ProcessedSMSData
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Loading function')


def get_data_class(record):
    if 'UpwardsUserCallData' in record.get('eventSourceARN'):
        return ProcessedCallData
    elif 'UpwardsUserSMSData' in record.get('eventSourceARN'):
        return ProcessedSMSData
    else:
        return None


def lambda_handler(event, context):
    db = Database()
    logger.info("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        if record.get('dynamodb', {}).get('NewImage'):
            data_class = get_data_class(record)
            if data_class:
                sql_query = data_class(
                    record['dynamodb']['NewImage']).sql_query
                db.execute_query(sql_query)
                logger.info('Data Processed')
            else:
                logger.info('Data record not found for SMS or Call')
        else:
            logger.info('Data record not found for Dynamodb')
    db.close_connection()
    return 'Successfully processed {} records.'.format(len(event['Records']))

a = [{
    'Records': [{
        'eventID': '1cc7ba0553f67f9c7382a29146ba58cf',
        'eventVersion': '1.1',
        'dynamodb': {
                'SequenceNumber': '10213900000000000323630440',
                'Keys': {
                    'created_at': {
                        'N': '19468831'
                    },
                    'customer_id': {
                        'S': '1'
                    }
                },
                'SizeBytes': 2691,
                'NewImage': {
                    'created_at': {
                        'N': '19468831'
                    },
                    'customer_id': {
                        'S': '1'
                    },
                    'data': {
                        'M': {
                            'data': {
                                'L': [{
                                    'M': {
                                        'Date': {
                                            'S': '1494224545000'
                                        },
                                        'Message': {
                                            'S': "Dear Rider, ease into the week with uberPOOL rides at Rs 49 flat upto 8 kms in Mumbai. Save more & get there comfortably, no matter where yo're going! TCA"
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-UBERIN'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494219622000'
                                        },
                                        'Message': {
                                            'S': "Warren Buffett FIRST ON ETNOW from 9.20am with Tanvir Gill at the Berkshire AGM. Hear him talk about his India plans and why he's so bullish on our economy"
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'DZ-066142'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494205217000'
                                        },
                                        'Message': {
                                            'S': 'Hello! Your a/c no 23529437 has been debited by Rs 14045 on 2017-05-08. The a/c balance is Rs 7232.42.Info: NEFT/MB/AXMB171288015122/prateek Agrawal'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-AxisBk'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494176064000'
                                        },
                                        'Message': {
                                            'S': 'Prateek Agrawal just received their first order and a coupon worth Rs. 50 redeemable on an order above Rs 400 has been added to your account. Happy eating'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-SWIGGY'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494146605000'
                                        },
                                        'Message': {
                                            'S': 'Your a/c 529437 is debited INR 58.48 on 07-05-2017 14:13:24 A/c Bal is INR 21277.42 Info: PUR/ZES*UBER INDIA SYSTEMS/NEW DELHI/ZES*UBER INDIA SYSTEMS/Seq No 949291'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-AxisBk'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494140482000'
                                        },
                                        'Message': {
                                            'S': "Here's how to get unlimited free delivery on all Swiggy orders till 12th May, Apply the coupon NOLIMIT. Order now on Swiggy! http://swig.gy/app"
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'HP-SWIGGY'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494080925000'
                                        },
                                        'Message': {
                                            'S': 'Massive savings on Holachef.com menu! Order right now to get the cheapest prices and save for the happy food tomorrow. #HolachefJoyExchang'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-HLACHF'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494078688000'
                                        },
                                        'Message': {
                                            'S': 'Mumbai, UberEATS is HERE! Link your Paytm Wallet on UberEATS & enjoy flat Rs.50 off on the first 5 orders. Code: EATNOW50. Click http://m.p-y.tm/ubet T&C appl'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-VPAYTM'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494068042000'
                                        },
                                        'Message': {
                                            'S': 'Dear Customer, Fresho Blueberry - Imported 125 gm , is now available at bigbasket. Happy shopping'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-BIGBKT'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494063912000'
                                        },
                                        'Message': {
                                            'S': 'Your order #1088910888 has been delivered. Thanks for using Swiggy. Issues? Reach out to us at www.swiggy.com/suppor'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-SWIGGY'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1494060823000'
                                        },
                                        'Message': {
                                            'S': 'Your order no. #1088910888 for Rs. 147 will be delivered shortly. Thanks for using Swiggy. Track your order here: http://swig.gy/AINQQ'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-SWIGGY'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1493979309000'
                                        },
                                        'Message': {
                                            'S': 'MakeMyTrip: Jet Airways Sale! FLAT 24% OFF only for today! Special Offer on HDFC Credit Cards - Book Now to get upto Rs.1500 Cashback to card. Code: HDFCDOM Visit applinks.makemytrip.com/SMsf/6Tb5sAHwTC Read T&Cs bit.ly/2qyydw'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'DZ-066033'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Date': {
                                            'S': '1493976687000'
                                        },
                                        'Message': {
                                            'S': 'Your order #1088781845 was delivered 20 minutes earlier than expected! Thanks for using Swiggy. Issues? Reach out to us at www.swiggy.com/suppor'
                                        },
                                        'Type': {
                                            'S': '1'
                                        },
                                        'Sender': {
                                            'S': 'AM-SWIGGY'
                                        }
                                    }
                                }]
                            },
                            'data_type': {
                                'S': 'sms'
                            }
                        }
                    }
                },
                'ApproximateCreationDateTime': 1494722220.0,
                'StreamViewType': 'NEW_IMAGE'
                },
        'awsRegion': 'us-west-2',
        'eventName': 'INSERT',
        'eventSourceARN': 'arn:aws:dynamodb:us-west-2:818172396282:table/UpwardsUserSMSData/stream/2017-05-13T14:53:47.353',
        'eventSource': 'aws:dynamodb'
    }]
},

    {
        'Records': [{
            'eventID': '2a83cd22ae50577270612c32fdf52c1f',
            'eventVersion': '1.1',
            'dynamodb': {
                'SequenceNumber': '15870600000000010555846588',
                'Keys': {
                    'created_at': {
                        'N': '19468831'
                    },
                    'customer_id': {
                        'S': '1'
                    }
                },
                'SizeBytes': 1626,
                'NewImage': {
                    'created_at': {
                        'N': '19468831'
                    },
                    'customer_id': {
                        'S': '1'
                    },
                    'data': {
                        'M': {
                            'data': {
                                'L': [{
                                    'M': {
                                        'Call Type': {
                                            'S': 'MISSED'
                                        },
                                        'Phone Number': {
                                            'S': '+91731306'
                                        },
                                        'Call Duration': {
                                            'S': '0'
                                        },
                                        'Call Date': {
                                            'S': '1491638509903'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+917313064000'
                                        },
                                        'Call Duration': {
                                            'S': '206'
                                        },
                                        'Call Date': {
                                            'S': '1491638566032'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+917021293314'
                                        },
                                        'Call Duration': {
                                            'S': '26'
                                        },
                                        'Call Date': {
                                            'S': '1491643213380'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'MISSED'
                                        },
                                        'Phone Number': {
                                            'S': '+911400460132'
                                        },
                                        'Call Duration': {
                                            'S': '0'
                                        },
                                        'Call Date': {
                                            'S': '1491726024965'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+919926898224'
                                        },
                                        'Call Duration': {
                                            'S': '711'
                                        },
                                        'Call Date': {
                                            'S': '1491749245286'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+91223302256'
                                        },
                                        'Call Duration': {
                                            'S': '20'
                                        },
                                        'Call Date': {
                                            'S': '1491753054918'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'OUTGOING'
                                        },
                                        'Phone Number': {
                                            'S': '08067464596'
                                        },
                                        'Call Duration': {
                                            'S': '0'
                                        },
                                        'Call Date': {
                                            'S': '1491823695054'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'OUTGOING'
                                        },
                                        'Phone Number': {
                                            'S': '08067467852'
                                        },
                                        'Call Duration': {
                                            'S': '0'
                                        },
                                        'Call Date': {
                                            'S': '1491823719250'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'OUTGOING'
                                        },
                                        'Phone Number': {
                                            'S': '08067467895'
                                        },
                                        'Call Duration': {
                                            'S': '0'
                                        },
                                        'Call Date': {
                                            'S': '1491823757196'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'OUTGOING'
                                        },
                                        'Phone Number': {
                                            'S': '08067462568'
                                        },
                                        'Call Duration': {
                                            'S': '115'
                                        },
                                        'Call Date': {
                                            'S': '1491823794770'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+918308844586'
                                        },
                                        'Call Duration': {
                                            'S': '110'
                                        },
                                        'Call Date': {
                                            'S': '1491903870936'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+917888041235'
                                        },
                                        'Call Duration': {
                                            'S': '12'
                                        },
                                        'Call Date': {
                                            'S': '1491908746187'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+917888044562'
                                        },
                                        'Call Duration': {
                                            'S': '46'
                                        },
                                        'Call Date': {
                                            'S': '1491908793915'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+919820132569'
                                        },
                                        'Call Duration': {
                                            'S': '18'
                                        },
                                        'Call Date': {
                                            'S': '1491916484103'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'MISSED'
                                        },
                                        'Phone Number': {
                                            'S': '+917021294589'
                                        },
                                        'Call Duration': {
                                            'S': '0'
                                        },
                                        'Call Date': {
                                            'S': '1491926803714'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'INCOMING'
                                        },
                                        'Phone Number': {
                                            'S': '+917021292134'
                                        },
                                        'Call Duration': {
                                            'S': '103'
                                        },
                                        'Call Date': {
                                            'S': '1491926899133'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'OUTGOING'
                                        },
                                        'Phone Number': {
                                            'S': '+919419257598'
                                        },
                                        'Call Duration': {
                                            'S': '0'
                                        },
                                        'Call Date': {
                                            'S': '1491984001598'
                                        }
                                    }
                                }, {
                                    'M': {
                                        'Call Type': {
                                            'S': 'OUTGOING'
                                        },
                                        'Phone Number': {
                                            'S': '180030004578'
                                        },
                                        'Call Duration': {
                                            'S': '0'
                                        },
                                        'Call Date': {
                                            'S': '1491986432333'
                                        }
                                    }
                                }]
                            },
                            'data_type': {
                                'S': 'call'
                            }
                        }
                    }
                },
                'ApproximateCreationDateTime': 1494722280.0,
                'StreamViewType': 'NEW_IMAGE'
            },
            'awsRegion': 'us-west-2',
            'eventName': 'INSERT',
            'eventSourceARN': 'arn:aws:dynamodb:us-west-2:818172396282:table/UpwardsUserCallData/stream/2017-05-13T14:53:17.798',
            'eventSource': 'aws:dynamodb'
        }]
}
]

for i in a:
    lambda_handler(i, 1)
