import random
import string


def get_list_of_unique_str(amount: int = 50, length: int = 24) -> list[str]:
    chars = string.ascii_letters + string.digits
    return [''.join(random.choices(chars, k=length)) for _ in range(amount)]


def get_value(_range: tuple = (1, 100)) -> int:
    return random.randint(*_range)


def get_objects(amount: int = None) -> str:
    amount = amount if amount else random.randint(1, 10)
    obj_template = "<object name='{}'/>"

    names_list = get_list_of_unique_str(amount=amount, length=12)
    names_str = "\n\t\t".join([obj_template.format(name) for name in names_list])

    return names_str
