from ddht.v5_1.alexandria.partials.proof import get_padding_elements


def test_get_padding_elements_1_15():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             P             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     P       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   P 0   1   0   1 0   ?   0   1 0   1   0   1 0   1

       ↑  |<-------------------PADDING--------------------->|
       |
       |
       ^-DATA
    """
    elements = get_padding_elements(0, 15, path_bit_length=4)
    assert len(elements) == 4
    (el_0, el_1, el_2, el_3,) = elements

    assert el_0.path == (False, False, False, True)
    assert el_1.path == (False, False, True)
    assert el_2.path == (False, True)
    assert el_3.path == (True,)


def test_get_padding_elements_2_14():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             P             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     P       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   1   0   1 0   1   0   1 0   1

      |  ↑  |<------------------PADDING-------------------->|
         |
         |
         ^-DATA
    """
    elements = get_padding_elements(1, 14, path_bit_length=4)
    assert len(elements) == 3
    (el_0, el_1, el_2,) = elements

    assert el_0.path == (False, False, True)
    assert el_1.path == (False, True)
    assert el_2.path == (True,)


def test_get_padding_elements_3_13():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             P             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   P   0   1 0   1   0   1 0   1   0   1 0   1

      |<-DATA->||<----------------PADDING------------------>|
    """
    elements = get_padding_elements(2, 13, path_bit_length=4)
    assert len(elements) == 3
    (el_0, el_1, el_2,) = elements

    assert el_0.path == (False, False, True, True)
    assert el_1.path == (False, True)
    assert el_2.path == (True,)


def test_get_padding_elements_4_12():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             P             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   1   0   1 0   1   0   1 0   1

      |<---DATA-->| |<--------------PADDING---------------->|
    """
    elements = get_padding_elements(3, 12, path_bit_length=4)
    assert len(elements) == 2
    (el_0, el_1,) = elements

    assert el_0.path == (False, True)
    assert el_1.path == (True,)


def test_get_padding_elements_6_10():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     P       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   1   0   1 0   1   0   1 0   1

      |<-------DATA------>|<-----------PADDING------------->|
    """
    elements = get_padding_elements(5, 10, path_bit_length=4)
    assert len(elements) == 2
    (el_0, el_1,) = elements

    assert el_0.path == (False, True, True)
    assert el_1.path == (True,)


def test_get_padding_elements_7_9():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   P   0   1 0   1   0   1 0   1

      |<--------DATA------->| |<----------PADDING---------->|
    """
    elements = get_padding_elements(6, 9, path_bit_length=4)
    assert len(elements) == 2
    (el_0, el_1,) = elements

    assert el_0.path == (False, True, True, True)
    assert el_1.path == (True,)


def test_get_padding_elements_8_8():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           P
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             1
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   P   0   1 0   1   0   1 0   1

      |<----------DATA--------->| |<--------PADDING-------->|
    """
    elements = get_padding_elements(7, 8, path_bit_length=4)
    assert len(elements) == 1
    (el_0,) = elements

    assert el_0.path == (True,)


def test_get_padding_elements_9_7():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           1
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             P
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     P       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   P   0   P 0   1   0   1 0   1

      |<------------DATA----------->| |<------PADDING------>|
    """
    elements = get_padding_elements(8, 7, path_bit_length=4)
    assert len(elements) == 3
    (el_0, el_1, el_2,) = elements

    assert el_0.path == (True, False, False, True)
    assert el_1.path == (True, False, True)
    assert el_2.path == (True, True)


def test_get_padding_elements_11_5():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           1
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             P
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   P   0   1 0   P   0   1 0   1

      |<---------------DATA-------------->| |<---PADDING--->|
    """
    elements = get_padding_elements(10, 5, path_bit_length=4)
    assert len(elements) == 2
    (el_0, el_1,) = elements

    assert el_0.path == (True, False, True, True)
    assert el_1.path == (True, True)


def test_get_padding_elements_15_1():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           1
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             P
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   P   0   1 0   P   0   1 0   P

      |<----------------------DATA--------------------->|  ↑
                                                           |
                                                           |
                                                   PADDING-^
    """
    elements = get_padding_elements(14, 1, path_bit_length=4)
    assert len(elements) == 1
    (el_0,) = elements

    assert el_0.path == (True, True, True, True)


def test_get_padding_elements_16_0():
    r"""
    0:                           X
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           1
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             P
           / \           / \           / \           / \
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: 0   1 0   1   0   1 0   P   0   1 0   P   0   1 0   P

      |<------------------------DATA----------------------->|

                           (NO PADDING)
    """
    elements = get_padding_elements(15, 0, path_bit_length=4)
    assert len(elements) == 0
