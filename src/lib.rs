use serde::Deserialize;
use serde_yaml::Value;
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
struct StageFile {
    stages: Vec<HashMap<String, Value>>,
}

type FnDeserializeStage<C> = Box<dyn Fn(Value) -> Box<dyn Stage<C = C>>>;

#[derive(Deserialize)]
pub struct StageManager<C> {
    #[serde(flatten)]
    file: StageFile,
    #[serde(skip)]
    deserialize_map: HashMap<String, FnDeserializeStage<C>>,
}

impl<C> StageManager<C> {
    pub fn from_str(yaml_str: &str) -> Self {
        Self {
            file: serde_yaml::from_str(yaml_str).unwrap(),
            deserialize_map: HashMap::new(),
        }
    }

    pub fn run(&self, context: C) -> C {
        let mut context = context;
        for s in &self.file.stages {
            s.into_iter()
                .map(|(k, v)| {
                    let f = &self.deserialize_map[k];
                    let mut s = f(v.clone());
                    s.setup();
                    s
                })
                .for_each(|s| {
                    s.run(&mut context);
                });
        }
        context
    }

    pub fn register_named<'a, S>(&mut self, name: &str) -> &mut Self
    where
        S: 'static + Stage<C = C> + Deserialize<'a>,
    {
        self.deserialize_map.insert(
            name.to_string(),
            Box::new(|v| Box::new(S::deserialize(v).unwrap())),
        );
        self
    }

    pub fn register<'a, S>(&mut self) -> &mut Self
    where
        S: 'static + Stage<C = C> + StageName + Deserialize<'a>,
    {
        self.register_named::<S>(S::stage_name())
    }
}

#[cfg(test)]
mod test {
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
        - add:
            x: 1
        - add:
            x: 2
        - add:
            x: 5
        "#;

        let mut m = StageManager::from_str(yaml_str);
        m.register::<Add>();

        let c = CalcContext { x: 1 };
        let c = m.run(c);

        assert_eq!(c.x, 9);
    }

    #[test]
    fn calc_mul_pipeline() {
        let yaml_str = r#"
        stages:
        - mul:
            x: 1
        - mul:
            x: 2
        - mul:
            x: 5
        "#;

        let mut m = StageManager::from_str(yaml_str);
        m.register::<Mul>();

        let c = CalcContext { x: 1 };
        let c = m.run(c);

        assert_eq!(c.x, 10);
    }

    #[test]
    fn calc_add_mul_pipeline() {
        let yaml_str = r#"
        stages:
        - mul:
            x: 1
        - add:
            x: 2
        # without '-' runs in random order
          add2:
            x: 10
        - mul:
            x: 5
        "#;

        let mut m = StageManager::from_str(yaml_str);
        m.register::<Mul>()
            .register::<Add>()
            .register_named::<Add>("add2");

        let c = CalcContext { x: 1 };
        let c = m.run(c);

        assert_eq!(c.x, 65);
    }

    #[test]
    fn stage_recursive_pipeline() {
        #[derive(Deserialize)]
        struct Top {
            #[serde(flatten)]
            stages: StageManager<CalcContext>,
        }

        impl Stage for Top {
            type C = CalcContext;

            fn setup(&mut self) {
                self.stages.register::<Add>().register::<Mul>();
            }

            fn run(&self, c: &mut Self::C) {
                *c = self.stages.run(c.clone());
            }
        }

        impl StageName for Top {
            fn stage_name() -> &'static str {
                "top"
            }
        }

        let yaml_str = r#"
        stages:
        - top:
            stages:
            - add:
                x: 1
            - add:
                x: 4
        - mul:
            x: 2
        "#;

        let mut m = StageManager::from_str(yaml_str);
        m.register::<Mul>().register::<Add>().register::<Top>();

        let c = CalcContext { x: 1 };
        let c = m.run(c);

        assert_eq!(c.x, 12);
    }
}
