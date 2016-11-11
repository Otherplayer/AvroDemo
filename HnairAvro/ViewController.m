//
//  ViewController.m
//  HnairAvro
//
//  Created by __无邪_ on 2016/11/11.
//  Copyright © 2016年 __无邪_. All rights reserved.
//


#import "ViewController.h"
#import "OAVAvroSerialization.h"

@interface ViewController ()

@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    // Do any additional setup after loading the view, typically from a nib.
    
    
//    OAVAvroSerialization *avro = [[OAVAvroSerialization alloc] init];
//    
////    NSString *personSchema = [self stringFromBundleResource:@"person_schema"];
//    NSString *peopleSchema = [self stringFromBundleResource:@"people_schema"];
//    
//    NSError *error;
////    BOOL result = [avro registerSchema:personSchema error:&error];
//    NSLog(@"%@",error);
////    NSLog(@"%d",result);
//    BOOL result = [avro registerSchema:peopleSchema error:&error];
//    NSLog(@"%@",error);
//    NSLog(@"%d",result);
//
//    
//    
//    NSDictionary *dict = [self JSONObjectFromBundleResource:@"people"];
//    
//    
//    NSData *data = [avro dataFromJSONObject:dict forSchemaNamed:@"People" error:&error];
//    
//    
//    NSDictionary *fromAvro = [avro JSONObjectFromData:data forSchemaNamed:@"People" error:&error];
//    
//    
//    
//    NSLog(@"%@",fromAvro);
    
    
    
    
    
    
    NSString *schema = @"{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"com.movile.objectiveavro.unittest.v1\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"country\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    
    OAVAvroSerialization *avro = [[OAVAvroSerialization alloc] init];
    NSError *error;
    BOOL result = [avro registerSchema:schema error:&error];
    
    NSDictionary *dict = @{@"name": @"Marcelo Fabri", @"country": @"Brazil", @"age": @20};
    NSData *data = [avro dataFromJSONObject:dict forSchemaNamed:@"Person" error:&error];
    
    
    
    NSLog(@"%d",result);
    NSLog(@"%@",data);
    
    
    NSDictionary *fromAvro = [avro JSONObjectFromData:data forSchemaNamed:@"Person" error:&error];
    
    
    
    
    NSLog(@"%d",result);
    NSLog(@"%@",fromAvro);
    
    
    
    
    
    
    
    
    
    
    
    
    
    
}


- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}





- (void)registerSchemas:(OAVAvroSerialization *)avro {
    NSString *personSchema = [self stringFromBundleResource:@"person_schema"];
    NSString *peopleSchema = [self stringFromBundleResource:@"people_schema"];
    
    [avro registerSchema:personSchema error:NULL];
    [avro registerSchema:peopleSchema error:NULL];
}






- (id)JSONObjectFromBundleResource:(NSString *)resource {
    NSString *path = [[NSBundle mainBundle] pathForResource:resource ofType:@"json"];
    NSData *data = [NSData dataWithContentsOfFile:path];
    
    NSDictionary *dict = [NSJSONSerialization JSONObjectWithData:data options:0 error:nil];
    return dict;
}
- (id)stringFromBundleResource:(NSString *)resource {
    NSString *path = [[NSBundle mainBundle] pathForResource:resource ofType:@"json"];
    return [NSString stringWithContentsOfFile:path encoding:NSUTF8StringEncoding error:nil];
}


@end
