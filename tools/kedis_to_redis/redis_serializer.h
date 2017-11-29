//
//  redis_serializer.h
//  kedis
//
//  Created by ziteng on 17/11/28.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __REDIS_SERIALIZER_H__
#define __REDIS_SERIALIZER_H__

#include "util.h"

int convert_kedis_serialized_value(const string& kedis_value, string& redis_value);

#endif /* __REDIS_SERIALIZER_H__ */
