use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

pub trait Stage {
    type C;

    fn run(&self, c: &mut Self::C);
    fn setup(&mut self) {}
}

pub trait StageName {
    fn stage_name() -> &'static str;
}

#[derive(Debug, Deserialize)]
pub struct StageFile<V> {
    stages: Vec<StageArgs<V>>,
}

#[derive(Debug, Deserialize)]
struct StageArgs<V> {
    name: String,
    args: V,
}

type FnDeserializeStage<C, V> = Box<dyn Fn(V) -> Box<dyn Stage<C = C>>>;

#[derive(Debug, Deserialize)]
pub struct StageManager<C, V> {
    #[serde(flatten)]
    file: StageFile<V>,

    #[serde(skip)]
    deserialize_map: HashMap<String, FnDeserializeStage<C, V>>,
}

impl<'de, C, V> StageManager<C, V>
where
    V: Deserialize<'de> + Deserializer<'de> + Clone,
{
    pub fn from_file(stage_file: StageFile<V>) -> Self {
        Self {
            file: stage_file,
            deserialize_map: HashMap::new(),
        }
    }

    pub fn run_stages(&self, context: &mut C) {
        self.file
            .stages
            .iter()
            .map(|s| {
                let f = &self.deserialize_map[&s.name];
                let mut s = f(s.args.clone());
                s.setup();
                s
            })
            .for_each(|s| {
                s.run(context);
            });
    }

    pub fn register_named<'a, S>(&mut self, name: &str) -> &mut Self
    where
        S: 'static + Stage<C = C> + Deserialize<'de>,
    {
        self.deserialize_map.insert(
            name.to_string(),
            Box::new(|v| Box::new(S::deserialize(v).unwrap())),
        );
        self
    }

    pub fn register<'a, S>(&mut self) -> &mut Self
    where
        S: 'static + Stage<C = C> + StageName + Deserialize<'de>,
    {
        self.register_named::<S>(S::stage_name())
    }
}

impl<'de, C, V> Stage for StageManager<C, V>
where
    V: Deserialize<'de> + Deserializer<'de> + Clone,
{
    type C = C;

    fn run(&self, c: &mut Self::C) {
        self.run_stages(c);
    }
}

#[cfg(test)]
mod test {
    use serde_yaml::Value;

    use super::*;

    #[derive(Debug, Default, Clone)]
    struct CalcContext {
        x: i64,
    }

    #[derive(Debug, Deserialize)]
    struct Add {
        x: i64,
    }

    impl Stage for Add {
        type C = CalcContext;

        fn run(&self, c: &mut Self::C) {
            c.x += self.x;
        }
    }

    impl StageName for Add {
        fn stage_name() -> &'static str {
            "add"
        }
    }

    #[derive(Debug, Deserialize)]
    struct Mul {
        x: i64,
    }

    impl StageName for Mul {
        fn stage_name() -> &'static str {
            "mul"
        }
    }

    impl Stage for Mul {
        type C = CalcContext;

        fn run(&self, c: &mut Self::C) {
            c.x *= self.x;
        }
    }

    #[test]
    fn calc_add_pipeline() {
        let yaml_str = r#"
        stages:
        - name: add
          args:
            x: 1
        - name: add
          args:
            x: 2
        - name: add
          args:
            x: 5
        "#;

        let file: StageFile<Value> = serde_yaml::from_str(yaml_str).unwrap();
        let mut m = StageManager::from_file(file);
        m.register::<Add>();

        let mut c = CalcContext { x: 1 };
        m.run(&mut c);

        assert_eq!(c.x, 9);
    }

    #[test]
    fn calc_mul_pipeline() {
        let yaml_str = r#"
        stages:
        - name: mul
          args:
            x: 1
        - name: mul
          args:
            x: 2
        - name: mul
          args:
            x: 5
        "#;

        let file: StageFile<Value> = serde_yaml::from_str(yaml_str).unwrap();
        let mut m = StageManager::from_file(file);
        m.register::<Mul>();

        let mut c = CalcContext { x: 1 };
        m.run(&mut c);

        assert_eq!(c.x, 10);
    }

    #[test]
    fn calc_add_mul_pipeline() {
        let yaml_str = r#"
        stages:
        - name: mul
          args:
            x: 1
        - name: add
          args:
            x: 2
        - name: mul
          args:
            x: 5
        "#;

        let file: StageFile<Value> = serde_yaml::from_str(yaml_str).unwrap();
        let mut m = StageManager::from_file(file);
        m.register::<Mul>().register::<Add>();

        let mut c = CalcContext { x: 1 };
        m.run(&mut c);

        assert_eq!(c.x, 15);
    }

    #[test]
    fn stage_recursive_pipeline() {
        #[derive(Deserialize)]
        struct Top {
            #[serde(flatten)]
            stages: StageManager<CalcContext, Value>,
        }

        impl Stage for Top {
            type C = CalcContext;

            fn setup(&mut self) {
                self.stages.register::<Add>().register::<Mul>();
            }

            fn run(&self, c: &mut Self::C) {
                self.stages.run(c);
            }
        }

        impl StageName for Top {
            fn stage_name() -> &'static str {
                "top"
            }
        }

        let yaml_str = r#"
        stages:
        - name: top 
          args:
            stages:
            - name: add
              args:
                x: 1
            - name: add
              args:
                x: 4
        - name: mul
          args:
            x: 2
        "#;

        let file: StageFile<Value> = serde_yaml::from_str(yaml_str).unwrap();
        let mut m = StageManager::from_file(file);
        m.register::<Mul>().register::<Add>().register::<Top>();

        let mut c = CalcContext { x: 1 };
        m.run(&mut c);

        assert_eq!(c.x, 12);
    }
}
