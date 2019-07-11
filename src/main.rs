//extern crate dxf;
use dxf::Drawing;
use dxf::entities::*;
use dxf::Point;

use serde::{Serialize, Deserialize, Serializer};
use yaml_rust::{yaml, YamlEmitter};
use rmp;
use byteorder::{BigEndian, WriteBytesExt};
use rmp_serde::Deserializer;
use std::io::Cursor;

#[macro_use] extern crate failure;
use failure::Error;

use std::fs::File;
use std::io::prelude::*;

use zmq;
use std::thread;
use std::time::Duration;

use std::collections::HashMap;
use rmp::Marker;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct DumbRequest {
    file: String
}

//enum MarkerEntityType {
//    Start,
//    End
//}
//
//struct MarkerEntity {
//    r#type: MarkerEntityType,
//}

/// Return size of a MessagePack ext header needed to encode len bytes of data
fn mp_ext_size_hint(len: u32) -> u32 {
    match len {
        1 => 2,
        2 => 2,
        4 => 2,
        8 => 2,
        16 => 2,
        _ => {
            if len <= 255 {
                3
            } else if len <= 65535 {
                4
            } else {
                6
            }
        }
    }
}

/// Return size of a MessagePack array header needed to encode len objects
fn mp_array_size_hint(len: u32) -> u32 {
    if len <= 15 {
        1
    } else if len <= 65535 {
        3
    } else {
        5
    }
}

trait VHApiSerializeable {
    fn serialize<W: Write>(&self, wr: &mut W) -> Result<(), Error>;
    fn size_hint(&self) -> usize;
}

#[derive(Debug)]
struct PointEntity {
    x: f32,
    y: f32,
    z: f32,
    r: u8,
    g: u8,
    b: u8
}

impl PointEntity {
    fn new() -> Self {
        PointEntity { x: 0f32, y: 0f32, z: 0f32, r: 0, g: 0, b: 0 }
    }
}

const POINT_ENTITY_EXT: i8      = 0x01;
const POLYLINE_ENTITY_EXT: i8   = 0x02;
const MARKER_ENTITY_EXT: i8     = 0x03;
const GROUP_ENTITY_EXT: i8      = 0x04;

impl VHApiSerializeable for PointEntity {
    fn serialize<W: Write>(&self, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_ext_meta(wr, 21, POINT_ENTITY_EXT)?;
        rmp::encode::write_f32(wr, self.x)?;
        rmp::encode::write_f32(wr, self.y)?;
        rmp::encode::write_f32(wr, self.z)?;
        rmp::encode::write_u8(wr, self.r)?;
        rmp::encode::write_u8(wr,self.g)?;
        rmp::encode::write_u8(wr,self.b)?;
        Ok(())
    }

    fn size_hint(&self) -> usize {
        24
    }
}

#[derive(Debug)]
struct PolylineEntityVertex {
    p: PointEntity,
    thickness: f32
}

#[derive(Debug)]
struct PolylineEntity {
    vertices: Vec<PolylineEntityVertex>
}

impl PolylineEntity {
    fn new() -> PolylineEntity {
        PolylineEntity { vertices: Vec::new() }
    }
}

#[derive(Debug)]
struct MarkerEntity {

}

impl MarkerEntity {
    fn new() -> MarkerEntity {
        MarkerEntity {}
    }
}

#[derive(Debug)]
enum BaseEntity {
    MarkerEntity(MarkerEntity),
    PointEntity(PointEntity),
    PolylineEntity(PolylineEntity),
    GroupEntity(GroupEntity)
}

#[derive(Debug)]
struct GroupEntity {
    entities: Vec<BaseEntity>
}

impl GroupEntity {
    fn new() -> GroupEntity {
        GroupEntity { entities: Vec::new() }
    }
}

fn pointentity_from_dxf_point(point: &Point) -> PointEntity {
    PointEntity {
        x: point.x as f32,
        y: point.y as f32,
        z: point.z as f32,
        r: 0xcc,
        g: 0xcc,
        b: 0xcc
    }
}

fn polylineentity_from_dxf_polyline(polyline: &Polyline) -> PolylineEntity {
    let mut pl = PolylineEntity::new();
    let len = polyline.vertices.len();
    for v in &polyline.vertices[..len - 1] {
        pl.vertices.push(PolylineEntityVertex {
            p: pointentity_from_dxf_point(&v.location),
            thickness: 1.0f32
        });
    }
    let last_pt = &polyline.vertices.get(len - 1).unwrap().location;
    let prev_pt = &polyline.vertices.get(len - 2).unwrap().location;
    if prev_pt != last_pt {
        // not closed polyline, keep last point
        pl.vertices.push(PolylineEntityVertex {
            p: pointentity_from_dxf_point(last_pt),
            thickness: 1.0f32
        });
    }
    pl
}

impl VHApiSerializeable for MarkerEntity {
    fn serialize<W: Write>(&self, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_ext_meta(wr, 2, MARKER_ENTITY_EXT)?;
        rmp::encode::write_u8(wr, 0)?;
        Ok(())
    }

    fn size_hint(&self) -> usize {
        4
    }
}

impl VHApiSerializeable for Point {
    fn serialize<W: Write>(&self, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_ext_meta(wr, 21, POINT_ENTITY_EXT)?;
        rmp::encode::write_f32(wr, self.x as f32)?;
        rmp::encode::write_f32(wr, self.y as f32)?;
        rmp::encode::write_f32(wr, self.z as f32)?;
        rmp::encode::write_u8(wr, 0xcc)?;
        rmp::encode::write_u8(wr, 0xcc)?;
        rmp::encode::write_u8(wr, 0xcc)?;
        Ok(())
    }

    fn size_hint(&self) -> usize {
        24
    }
}

impl VHApiSerializeable for &Polyline {
    fn serialize<W: Write>(&self, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_ext_meta(wr, self.size_hint() as u32, POLYLINE_ENTITY_EXT)?;
        rmp::encode::write_array_len(wr, self.vertices.len() as u32)?;
        for v in &self.vertices {
            VHApiSerializeable::serialize(&v.location, wr)?;
            rmp::encode::write_f32(wr, 1.0f32 as f32)?; // check v.starting_width
        }
        Ok(())
    }

    fn size_hint(&self) -> usize {
        let pt = PointEntity::new();
        let datalen = self.vertices.len() * (5 + pt.size_hint());
        mp_array_size_hint(self.vertices.len() as u32) as usize +
            datalen +
            mp_ext_size_hint(datalen as u32) as usize
    }
}

impl VHApiSerializeable for PolylineEntity {
    fn serialize<W: Write>(&self, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_ext_meta(wr, self.size_hint() as u32, POLYLINE_ENTITY_EXT)?;
        rmp::encode::write_array_len(wr, self.vertices.len() as u32)?;
        for v in &self.vertices {
            v.p.serialize(wr)?;
            rmp::encode::write_f32(wr, v.thickness)?;
        }
        Ok(())
    }

    fn size_hint(&self) -> usize {
        let pt = PointEntity::new();
        let datalen = self.vertices.len() * (5 + pt.size_hint());
        mp_array_size_hint(self.vertices.len() as u32) as usize +
            datalen +
            mp_ext_size_hint(datalen as u32) as usize
    }
}

impl VHApiSerializeable for BaseEntity {
    fn serialize<W: Write>(&self, wr: &mut W) -> Result<(), Error> {
        match self {
            BaseEntity::MarkerEntity(m) => {
                m.serialize(wr)?;
            },
            BaseEntity::GroupEntity(g) => {
                g.serialize(wr)?;
            },
            BaseEntity::PolylineEntity(pl) => {
                pl.serialize(wr)?;
            },
            BaseEntity::PointEntity(p) => {
                p.serialize(wr)?;
            }
        }
        Ok(())
    }

    fn size_hint(&self) -> usize {
        match self {
            BaseEntity::MarkerEntity(m) => {
                m.size_hint()
            },
            BaseEntity::GroupEntity(g) => {
                g.size_hint()
            },
            BaseEntity::PolylineEntity(pl) => {
                pl.size_hint()
            },
            BaseEntity::PointEntity(p) => {
                p.size_hint()
            }
        }
    }
}

impl VHApiSerializeable for GroupEntity {
    fn serialize<W: Write>(&self, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_ext_meta(wr, self.size_hint() as u32, GROUP_ENTITY_EXT)?;
        rmp::encode::write_array_len(wr, self.entities.len() as u32)?;
        for entity in &self.entities {
            entity.serialize(wr)?;
        }
        Ok(())
    }

    fn size_hint(&self) -> usize {
        let mut length: usize = 0;
        for entity in &self.entities {
            length = length + entity.size_hint()
        }
        mp_ext_size_hint(length as u32) as usize +
            mp_array_size_hint(self.entities.len() as u32) as usize +
            length
    }
}

fn main() -> Result<(), Error> {
//    let drawing = Drawing::load_file("/Users/roman/Desktop/test.dxf")?;
//
//    let mut blocks: HashMap<String, GroupEntity> = HashMap::new();
//    for block in &drawing.blocks {
//        println!("block: {}", block.name);
//        let mut group = GroupEntity::new();
//        for e in &block.entities {
//            let mut unknown = false;
//            match &e.specific {
//                EntityType::Polyline(dxfpl) => {
//                    group.entities.push(BaseEntity::PolylineEntity(polylineentity_from_dxf_polyline(dxfpl)));
//                },
//                _ => {
//                    unknown = true;
//                }
//            }
//            if unknown {
//                println!("block: {} unknown entity", block.name);
//            }
//        }
//        blocks.insert(block.name.clone(), group);
//    }
//
//    for entity in &drawing.entities {
//        match &entity.specific {
//            EntityType::Insert(insert) => {
//                println!("insert: {}, {:?}", insert.name, blocks[&insert.name]);
//            }
//            _ => {
//
//            }
//        }
//    }

//    let pt1 = PointEntity::new();
//    let pt2 = PointEntity::new();
//    let m1 = MarkerEntity::new();
//    let mut group = GroupEntity::new();
//    group.entities.push(BaseEntity::PointEntity(pt1));
//    group.entities.push(BaseEntity::PointEntity(pt2));
//    group.entities.push(BaseEntity::MarkerEntity(m1));
//    let mut buf = Vec::new();
//    group.serialize(&mut buf);
//    println!("hint: {} real: {}", group.size_hint(), buf.len());
//    println!("{:x?}", buf);

    //let mut file = File::create("/Users/roman/Desktop/test.yaml")?;
    //file.write_all(serde_yaml::to_string(&drawing.blocks).unwrap().as_bytes());

    let context = zmq::Context::new();
    let responder = context.socket(zmq::REP)?;
    responder
        .bind("tcp://*:5555")
        .expect("bind failed");
    let publisher = context.socket(zmq::PUB)?;
    publisher
        .bind("tcp://*:5556")
        .expect("bind failed 6");

    loop {
        let bytes = responder.recv_bytes(0).unwrap();
        println!("Got a req: {}", bytes.len());
        let cur = Cursor::new(bytes);
        let mut de = Deserializer::new(cur);
        let deser: DumbRequest = Deserialize::deserialize(&mut de).unwrap();
        println!("Deser: {:?}", deser);

        let drawing = Drawing::load_file(&deser.file)?;

        let mut blocks: HashMap<String, GroupEntity> = HashMap::new();
        for block in &drawing.blocks {
            println!("block: {}", block.name);
            let mut group = GroupEntity::new();
            for e in &block.entities {
                let mut unknown = false;
                match &e.specific {
                    EntityType::Polyline(dxfpl) => {
                        group.entities.push(BaseEntity::PolylineEntity(polylineentity_from_dxf_polyline(dxfpl)));
                    },
                    _ => {
                        unknown = true;
                    }
                }
                if unknown {
                    println!("block: {} unknown entity {:?}", block.name, e.specific);
                }
            }
            blocks.insert(block.name.clone(), group);
        }

        publisher.send("entities/start".as_bytes(), 0)?;

        let mut entities_marker = Vec::new();
        entities_marker.write("entities".as_bytes())?;

        let stream_marker = MarkerEntity {};
        let mut buf = Vec::new();
        stream_marker.serialize(&mut buf)?;
        publisher.send_multipart(&[entities_marker.clone(), buf], 0)?;

        for e in drawing.entities {
            let mut unknown_entity = false;
            match &e.specific {
                EntityType::Polyline(p) => {
                    println!("found polyline");

                    let mut buf = Vec::new();
                    VHApiSerializeable::serialize(&p, &mut buf)?;
                    println!("sending {} bytes", buf.len());
                    publisher.send_multipart(&[entities_marker.clone(), buf], 0)?;
                }
                EntityType::Insert(insert) => {
                    println!("found insert");

                    let mut buf = Vec::new();
                    blocks[&insert.name].serialize(&mut buf)?;
                    println!("sending {} bytes", buf.len());
                    publisher.send_multipart(&[entities_marker.clone(), buf], 0)?;
                }
                _ => {
                    unknown_entity = true;
                }
            }
            if unknown_entity {
                let res_ser = serde_yaml::to_string(&e.specific).unwrap();
                println!("unknown: {}", res_ser);
            }
        }

        publisher.send("entities/end".as_bytes(), 0)?;

        std::thread::sleep(Duration::from_millis(1_000));
        responder.send("World", 0)?;
    }


    Ok(())
}
