use crate::errors::ValidationError;

pub fn validate_comma_seperated_list<E>(arg: &str, field_name: &str) -> Result<(), E>
where
    E: ValidationError,
{
    if arg.contains(' ') {
        return Err(E::validation_error(format!(
            "{} must be a comma separated list of field names without spaces",
            field_name
        )));
    }
    Ok(())
}
